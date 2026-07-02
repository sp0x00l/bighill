package k8s

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"

	"model_serving_service/pkg/domain/model"

	log "github.com/sirupsen/logrus"
)

func WorkloadName(servedModel *model.ServedModel) string {
	log.Trace("WorkloadName")

	if strings.TrimSpace(servedModel.ResourceName) != "" {
		return dns1123Name(servedModel.ResourceName)
	}
	return dns1123Name(fmt.Sprintf("served-model-%s-v%d", servedModel.ModelID.String(), servedModel.ModelVersion))
}

func ServingModelName(servedModel *model.ServedModel) string {
	log.Trace("ServingModelName")

	if strings.TrimSpace(servedModel.ServingModel) != "" {
		return strings.TrimSpace(servedModel.ServingModel)
	}
	return dns1123Name(fmt.Sprintf("%s-v%d-%s", servedModel.Name, servedModel.ModelVersion, servedModel.ModelID.String()[:8]))
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

var invalidKubernetesNameChars = regexp.MustCompile(`[^a-z0-9-]+`)

const maxKubernetesNameLength = 63
