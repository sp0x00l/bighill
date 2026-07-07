package model

type ListFilter struct {
	Kind      ModelKind
	KindSet   bool
	Source    ModelSource
	SourceSet bool
	Status    ModelStatus
	StatusSet bool
	Trainable bool
}
