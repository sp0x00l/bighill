package app

import log "github.com/sirupsen/logrus"

type DedupeStore struct {
	limit int
	order []string
	seen  map[string]struct{}
}

func NewDedupeStore(limit int) *DedupeStore {
	log.Trace("NewDedupeStore")

	return &DedupeStore{
		limit: limit,
		seen:  map[string]struct{}{},
	}
}

func (s *DedupeStore) Seen(id string) bool {
	log.Trace("DedupeStore Seen")

	if id == "" {
		return false
	}
	if _, ok := s.seen[id]; ok {
		return true
	}
	s.seen[id] = struct{}{}
	s.order = append(s.order, id)
	for s.limit > 0 && len(s.order) > s.limit {
		evict := s.order[0]
		s.order = s.order[1:]
		delete(s.seen, evict)
	}
	return false
}
