package servedmodel

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
	log "github.com/sirupsen/logrus"
)

type Spec struct {
	ModelID          string `json:"model_id"`
	TrainingRunID    string `json:"training_run_id"`
	DatasetID        string `json:"dataset_id"`
	Name             string `json:"name"`
	ModelVersion     int    `json:"model_version"`
	BaseModel        string `json:"base_model"`
	ArtifactLocation string `json:"artifact_location"`
	ArtifactFormat   string `json:"artifact_format"`
	ArtifactChecksum string `json:"artifact_checksum"`
	AdapterURI       string `json:"adapter_uri"`
	ServingTarget    string `json:"serving_target"`
	ServingModel     string `json:"serving_model"`
}

type Status struct {
	ServingLoadStatus  string `json:"serving_load_status"`
	ServingTarget      string `json:"serving_target"`
	ServingModel       string `json:"serving_model"`
	FailureReason      string `json:"failure_reason"`
	ObservedGeneration int64  `json:"observed_generation"`
	ReadyReplicas      int32  `json:"ready_replicas"`
}

type Record struct {
	Name       string    `json:"name"`
	Namespace  string    `json:"namespace"`
	Generation int64     `json:"generation"`
	Spec       Spec      `json:"spec"`
	Status     Status    `json:"status"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type Store struct {
	path string
}

func NewStore(path string) (*Store, error) {
	log.Trace("servedmodel NewStore")

	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("served model local store path is required")
	}
	path = filepath.Clean(path)
	if err := os.MkdirAll(filepath.Dir(path), os.ModePerm); err != nil {
		return nil, err
	}
	return &Store{path: path}, nil
}

func ResourceName(modelID string, modelVersion int) string {
	log.Trace("servedmodel ResourceName")

	return dns1123Name(fmt.Sprintf("served-model-%s-v%d", modelID, modelVersion))
}

func (s *Store) UpsertSpec(name string, namespace string, spec Spec) error {
	log.Trace("servedmodel Store UpsertSpec")

	return s.withDB(func(db *database) error {
		record := db.Items[name]
		now := time.Now().UTC()
		if record.Name == "" {
			db.ResourceVersion++
			record = Record{
				Name:       name,
				Namespace:  namespace,
				Generation: 1,
				Spec:       spec,
				UpdatedAt:  now,
			}
			db.Items[name] = record
			return nil
		}
		if reflect.DeepEqual(record.Spec, spec) && record.Namespace == namespace {
			return nil
		}
		db.ResourceVersion++
		record.Namespace = namespace
		record.Generation++
		record.Spec = spec
		record.Status = Status{}
		record.UpdatedAt = now
		db.Items[name] = record
		return nil
	})
}

func (s *Store) UpdateStatus(name string, status Status) error {
	log.Trace("servedmodel Store UpdateStatus")

	return s.withDB(func(db *database) error {
		record, ok := db.Items[name]
		if !ok {
			return ErrNotFound
		}
		if status.ObservedGeneration != record.Generation {
			return fmt.Errorf("%w: served model %s status observed generation %d does not match current generation %d", ErrStaleGeneration, name, status.ObservedGeneration, record.Generation)
		}
		if reflect.DeepEqual(record.Status, status) {
			return nil
		}
		db.ResourceVersion++
		record.Status = status
		record.UpdatedAt = time.Now().UTC()
		db.Items[name] = record
		return nil
	})
}

func (s *Store) Read(name string) (Record, bool, error) {
	log.Trace("servedmodel Store Read")

	var out Record
	var ok bool
	err := s.withReadDB(func(db *database) error {
		out, ok = db.Items[name]
		return nil
	})
	return out, ok, err
}

func (s *Store) List(namespace string) ([]Record, string, error) {
	log.Trace("servedmodel Store List")

	out := []Record{}
	resourceVersion := "0"
	err := s.withReadDB(func(db *database) error {
		resourceVersion = strconvFormatInt(db.ResourceVersion)
		total := 0
		for _, record := range db.Items {
			total++
			if namespace == "" || record.Namespace == namespace {
				out = append(out, record)
			}
		}
		if namespace != "" && len(out) == 0 && total > 0 {
			return fmt.Errorf("%w: no served models found for namespace %q; local store contains records for other namespaces", ErrNamespaceMismatch, namespace)
		}
		return nil
	})
	return out, resourceVersion, err
}

func (s *Store) Watch(ctx context.Context, namespace string, resourceVersion string) (<-chan string, error) {
	log.Trace("servedmodel Store Watch")

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	if err := watcher.Add(filepath.Dir(s.path)); err != nil {
		watcher.Close()
		return nil, err
	}

	changes := make(chan string, 1)
	go func() {
		defer close(changes)
		defer watcher.Close()

		lastResourceVersion := strings.TrimSpace(resourceVersion)
		if ok := s.emitIfChanged(ctx, namespace, &lastResourceVersion, changes); !ok {
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.WithError(err).Warn("served model local store watch failed")
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if !s.isStoreFileEvent(event.Name) {
					continue
				}
				if ok := s.emitIfChanged(ctx, namespace, &lastResourceVersion, changes); !ok {
					return
				}
			}
		}
	}()
	return changes, nil
}

func (s *Store) Reset() error {
	log.Trace("servedmodel Store Reset")

	return s.withDB(func(db *database) error {
		db.ResourceVersion++
		db.Items = map[string]Record{}
		return nil
	})
}

func (s *Store) withReadDB(fn func(*database) error) error {
	log.Trace("servedmodel Store withReadDB")

	return s.withLockedDB(false, fn)
}

func (s *Store) withDB(fn func(*database) error) error {
	log.Trace("servedmodel Store withDB")

	return s.withLockedDB(true, fn)
}

func (s *Store) withLockedDB(write bool, fn func(*database) error) error {
	log.Trace("servedmodel Store withLockedDB")

	lockPath := s.path + ".lock"
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o666)
	if err != nil {
		return err
	}
	defer lockFile.Close()
	lockMode := syscall.LOCK_SH
	if write {
		lockMode = syscall.LOCK_EX
	}
	if err := syscall.Flock(int(lockFile.Fd()), lockMode); err != nil {
		return err
	}
	defer syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)

	db, err := s.readDB()
	if err != nil {
		return err
	}
	if err := fn(db); err != nil {
		return err
	}
	if !write {
		return nil
	}
	return s.writeDB(db)
}

func (s *Store) emitIfChanged(ctx context.Context, namespace string, lastResourceVersion *string, changes chan<- string) bool {
	log.Trace("servedmodel Store emitIfChanged")

	_, nextResourceVersion, err := s.List(namespace)
	if err != nil {
		log.WithContext(ctx).WithError(err).Warn("served model local store watch ignored event")
		return true
	}
	if nextResourceVersion == *lastResourceVersion {
		return true
	}
	*lastResourceVersion = nextResourceVersion
	select {
	case <-ctx.Done():
		return false
	case changes <- nextResourceVersion:
	default:
	}
	return true
}

func (s *Store) readDB() (*database, error) {
	log.Trace("servedmodel Store readDB")

	raw, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return &database{Items: map[string]Record{}}, nil
	}
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return &database{Items: map[string]Record{}}, nil
	}
	var db database
	if err := json.Unmarshal(raw, &db); err != nil {
		return nil, err
	}
	if db.Items == nil {
		db.Items = map[string]Record{}
	}
	return &db, nil
}

func (s *Store) writeDB(db *database) error {
	log.Trace("servedmodel Store writeDB")

	raw, err := json.MarshalIndent(db, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o666); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

func (s *Store) isStoreFileEvent(path string) bool {
	log.Trace("servedmodel Store isStoreFileEvent")

	clean := filepath.Clean(path)
	return clean == s.path || clean == s.path+".tmp"
}

type database struct {
	ResourceVersion int64             `json:"resource_version"`
	Items           map[string]Record `json:"items"`
}

func strconvFormatInt(value int64) string {
	log.Trace("servedmodel strconvFormatInt")

	return fmt.Sprintf("%d", value)
}

func dns1123Name(value string) string {
	log.Trace("servedmodel dns1123Name")

	name := strings.ToLower(value)
	name = invalidNameChars.ReplaceAllString(name, "-")
	name = strings.Trim(name, "-")
	if name == "" {
		name = "served-model"
	}
	if len(name) <= maxNameLength {
		return name
	}
	sum := sha1.Sum([]byte(name))
	suffix := hex.EncodeToString(sum[:])[:10]
	prefix := strings.Trim(name[:maxNameLength-len(suffix)-1], "-")
	if prefix == "" {
		prefix = "served-model"
	}
	return prefix + "-" + suffix
}

var (
	ErrNotFound          = errors.New("served model not found")
	ErrStaleGeneration   = errors.New("stale served model generation")
	ErrNamespaceMismatch = errors.New("served model namespace mismatch")
	invalidNameChars     = regexp.MustCompile(`[^a-z0-9-]+`)
)

const maxNameLength = 63
