package model

import (
	"time"

	"github.com/google/uuid"
)

type BaseRuntime struct {
	ResourceName   string
	Namespace      string
	Generation     int64
	BaseModel      string
	PoolKey        string
	MaxLoras       int
	MaxLoraRank    int
	GPU            string
	Image          string
	Endpoint       string
	Phase          string
	ReadyReplicas  int32
	LoadedAdapters []BaseRuntimeLoadedAdapter
}

type BaseRuntimeLoadedAdapter struct {
	ServingModel            string
	ServedModelResourceName string
	ModelID                 uuid.UUID
	ObservedGeneration      int64
	LastUsedAt              time.Time
	Pinned                  bool
}
