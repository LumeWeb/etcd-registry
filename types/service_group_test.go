package types

import (
	clientv3 "go.etcd.io/etcd/client/v3"
	"testing"
)

func TestServiceGroup_Configure(t *testing.T) {
	mock, ctrl := NewTestMockRegistry(t)
	defer ctrl.Finish()

	// Setup common expectations
	mock.EXPECT().GetClient().Return(&clientv3.Client{}).AnyTimes()
	mock.EXPECT().GetEtcdBasePath().Return("/test").AnyTimes()

	sg := &ServiceGroup{
		Registry: mock,
		Name:     "test-group",
		Spec:     ServiceGroupSpec{},
	}

	if sg.Name != "test-group" {
		t.Errorf("Expected group name to be 'test-group', got %s", sg.Name)
	}
}
