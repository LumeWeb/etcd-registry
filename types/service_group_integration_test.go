//go:build integration
package types

import (
	"context"
	"testing"
	"time"
)

func TestServiceGroup_Integration(t *testing.T) {
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

	// Test GetNodes functionality
	t.Run("GetNodes", func(t *testing.T) {
		mock, ctrl := NewTestMockRegistry(t)
		defer ctrl.Finish()

		mock.EXPECT().GetClient().Return(&clientv3.Client{}).AnyTimes()
		mock.EXPECT().GetEtcdBasePath().Return("/test").AnyTimes()

		sg := &ServiceGroup{
			Registry: mock,
			Name:     "test-group",
			Spec:     ServiceGroupSpec{},
		}

		// Test successful node retrieval
		t.Run("Successful_Retrieval", func(t *testing.T) {
			// Register some test nodes first
			testNodes := []Node{
				{
					ID:           "node1",
					ExporterType: "test",
					Port:         9100,
					MetricsPath:  "/metrics",
					Labels:       map[string]string{"env": "prod"},
				},
				{
					ID:           "node2",
					ExporterType: "test",
					Port:         9101,
					MetricsPath:  "/metrics",
					Labels:       map[string]string{"env": "dev"},
				},
			}

			for _, node := range testNodes {
				done, errChan, err := sg.RegisterNode(context.Background(), node, 10*time.Second)
				if err != nil {
					t.Fatalf("Failed to register test node: %v", err)
				}
				select {
				case <-done:
					// Success
				case err := <-errChan:
					t.Fatalf("Node registration failed: %v", err)
				case <-time.After(time.Second):
					t.Fatal("Registration timed out")
				}
			}

			nodes, err := sg.GetNodes(context.Background())
			if err != nil {
				t.Fatalf("GetNodes failed: %v", err)
			}

			if len(nodes) != len(testNodes) {
				t.Errorf("Expected %d nodes, got %d", len(testNodes), len(nodes))
			}

			// Verify node contents
			nodeMap := make(map[string]Node)
			for _, n := range nodes {
				nodeMap[n.ID] = n
			}

			for _, expected := range testNodes {
				got, exists := nodeMap[expected.ID]
				if !exists {
					t.Errorf("Node %s not found in results", expected.ID)
					continue
				}
				if !reflect.DeepEqual(got, expected) {
					t.Errorf("Node mismatch for %s:\nwant: %+v\ngot:  %+v", expected.ID, expected, got)
				}
			}
		})
	})
}
