package mocks

import "data_registry_service/pkg/domain/model"

type MockSourceConfig struct {
	GetStorageTypeCalled bool

	NextSourceType model.StorageType
}

func (m *MockSourceConfig) GetStorageType() model.StorageType {
	m.GetStorageTypeCalled = true

	return m.NextSourceType
}
