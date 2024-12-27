package types

import (
	"testing"
	"go.uber.org/mock/gomock"
)

// Helper function to create a new mock registry with controller
func NewTestMockRegistry(t *testing.T) (*MockRegistry, *gomock.Controller) {
	ctrl := gomock.NewController(t)
	mock := NewMockRegistry(ctrl)
	return mock, ctrl
}
