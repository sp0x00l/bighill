package k8s

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"

	"model_serving_service/pkg/domain/model"

	servedmodelstore "lib/shared_lib/servedmodel"

	log "github.com/sirupsen/logrus"
)

func WorkloadName(servedModel *model.ServedModel) string {
	log.Trace("WorkloadName")

	if strings.TrimSpace(servedModel.ResourceName) != "" {
		return dns1123Name(servedModel.ResourceName)
	}
	return servedmodelstore.ResourceName(servedModel.ModelID.String(), servedModel.ModelVersion)
}

func SharedRuntimeWorkloadName(servedModel *model.ServedModel) string {
	log.Trace("SharedRuntimeWorkloadName")

	return dns1123NameWithHash("served-runtime", servedModel.BaseModel)
}

func ServingModelName(servedModel *model.ServedModel) string {
	log.Trace("ServingModelName")

	if strings.TrimSpace(servedModel.ServingModel) != "" {
		return strings.TrimSpace(servedModel.ServingModel)
	}
	return dns1123Name(fmt.Sprintf("%s-v%d-%s", servedModel.Name, servedModel.ModelVersion, servedModel.ModelID.String()[:8]))
}

func SharedRuntimeServingModelName(servedModel *model.ServedModel) string {
	log.Trace("SharedRuntimeServingModelName")

	return dns1123NameWithHash("base", servedModel.BaseModel)
}

func ServiceEndpoint(namespace, serviceName string, port int32) string {
	log.Trace("ServiceEndpoint")

	return fmt.Sprintf("http://%s.%s.svc.cluster.local:%d", serviceName, namespace, port)
}

func dns1123Name(value string) string {
	log.Trace("dns1123Name")

	name := strings.ToLower(value)
	name = invalidKubernetesNameChars.ReplaceAllString(name, "-")
	name = strings.Trim(name, "-")
	if name == "" {
		name = "served-model"
	}
	if len(name) <= maxKubernetesNameLength {
		return name
	}
	sum := sha1.Sum([]byte(name))
	suffix := hex.EncodeToString(sum[:])[:10]
	prefix := strings.Trim(name[:maxKubernetesNameLength-len(suffix)-1], "-")
	if prefix == "" {
		prefix = "served-model"
	}
	return prefix + "-" + suffix
}

func dns1123NameWithHash(prefix string, value string) string {
	log.Trace("dns1123NameWithHash")

	source := strings.TrimSpace(value)
	normalized := dns1123Name(source)
	sum := sha1.Sum([]byte(source))
	suffix := hex.EncodeToString(sum[:])[:10]
	base := dns1123Name(fmt.Sprintf("%s-%s", prefix, normalized))
	maxPrefixLength := maxKubernetesNameLength - len(suffix) - 1
	if len(base) > maxPrefixLength {
		base = strings.Trim(base[:maxPrefixLength], "-")
	}
	if base == "" {
		base = dns1123Name(prefix)
	}
	return base + "-" + suffix
}

var invalidKubernetesNameChars = regexp.MustCompile(`[^a-z0-9-]+`)

const maxKubernetesNameLength = 63
