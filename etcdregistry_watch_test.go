//go:build integration

package etcdregistry

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.lumeweb.com/etcd-registry/types"
)

func TestWatchServices(t *testing.T) {
	r, ctx, cleanup := setupTest(t)
	defer cleanup()

	// Start watching for events
	events, err := r.WatchServices(ctx)
	if err != nil {
		t.Fatalf("Failed to start watch: %v", err)
	}

	// Create a test group and node
	group, err := r.CreateOrJoinServiceGroup(ctx, "watch-test-group")
	if err != nil {
		t.Fatalf("Failed to create test group: %v", err)
	}

	testNode := types.Node{
		ID:           "watch-test-node",
		ExporterType: "test_exporter",
		Port:         9100,
		MetricsPath:  "/metrics",
		Labels:       map[string]string{"test": "watch"},
	}

	// Create channels for synchronization
	eventReceived := make(chan struct{})
	watchErrors := make(chan error, 1)

	// Start watching for events in a goroutine with better error handling
	go func() {
		defer close(eventReceived)
		for event := range events {
			if event.Type == types.EventTypeCreate && event.Node != nil {
				t.Logf("Received event: type=%s group=%s nodeID=%s",
					event.Type, event.GroupName, event.Node.ID)

				if event.Node.ID == testNode.ID {
					// Verify event details
					if event.GroupName != "watch-test-group" {
						watchErrors <- fmt.Errorf("expected group name 'watch-test-group', got %s", event.GroupName)
						return
					}
					if event.Node.ExporterType != testNode.ExporterType {
						watchErrors <- fmt.Errorf("expected exporter type %s, got %s", testNode.ExporterType, event.Node.ExporterType)
						return
					}
					eventReceived <- struct{}{}
					return
				}
			}
		}
		t.Log("Watch events channel closed")
	}()

	// Register the node after setting up the watch
	nodeDone, errChan, err := group.RegisterNode(ctx, testNode, 10*time.Second)
	if err != nil {
		t.Fatalf("Failed to register node: %v", err)
	}

	// Wait for registration completion first
	select {
	case <-nodeDone:
		t.Log("Node registration completed")
	case err := <-errChan:
		t.Fatalf("Node registration failed: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("Node registration timed out")
	}

	// Give etcd a moment to propagate the changes
	time.Sleep(time.Second)

	// Then wait for the watch event
	select {
	case <-eventReceived:
		t.Log("Successfully received watch event")
	case err := <-watchErrors:
		t.Fatalf("Watch error: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("Timed out waiting for watch event")
	}

	// Single cleanup attempt with error handling
	cleanupCtx := context.Background()
	if err := r.DeleteNode(cleanupCtx, "watch-test-group", testNode); err != nil {
		if !strings.Contains(err.Error(), "node not found") {
			t.Errorf("Cleanup failed: %v", err)
		}
	} else {
		t.Log("Successfully cleaned up test node")
	}
	t.Log("Cleanup completed")
}
