//go:build integration
package etcdregistry

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.lumeweb.com/etcd-registry/types"
	"strings"
	"testing"
	"time"
)

// setupTest creates a new registry and returns cleanup function
func setupTest(t *testing.T) (*EtcdRegistry, context.Context, func()) {
	logger := logrus.WithFields(logrus.Fields{
		"component": "etcd-registry-test",
		"test":      t.Name(),
	})

	config := clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 10 * time.Second,
	}

	// Create etcd client directly to check/configure auth
	client, err := clientv3.New(config)
	if err != nil {
		t.Fatalf("Failed to create etcd client: %v", err)
	}
	defer client.Close()

	// Disable authentication if enabled
	authCtx, authCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer authCancel()
	
	authStatus, err := client.AuthStatus(authCtx)
	if err == nil && authStatus.Enabled {
		if _, err := client.AuthDisable(authCtx); err != nil {
			t.Fatalf("Failed to disable auth: %v", err)
		}
	}

	r, err := NewEtcdRegistry([]string{"localhost:2379"}, "/test", "", "", 10*time.Second, 3)
	if err != nil {
		t.Fatalf("Failed to create registry: %v", err)
	}

	// Set log level to error only for integration tests
	logrus.SetLevel(logrus.ErrorLevel)
	if err != nil {
		t.Fatalf("Failed to create registry: %v", err)
	}
	r.logger = logger

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	cleanup := func() {
		cancel()
		if r.client != nil {
			cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cleanCancel()
			_, _ = r.client.Delete(cleanCtx, r.etcdBasePath, clientv3.WithPrefix())
		}
		if err := r.Close(); err != nil {
			t.Errorf("Failed to close registry: %v", err)
		}
	}

	return r, ctx, cleanup
}

func TestRegisterNode(t *testing.T) {
	t.Parallel()
	r, _, cleanup := setupTest(t)
	defer cleanup()

	tests := []struct {
		name        string
		node        types.Node
		groupName   string
		groupLabels map[string]string
		ttl         time.Duration
		expectError bool
		errorMsg    string
		validate    func(*testing.T, *EtcdRegistry, types.Node) error
	}{
		{
			name: "Valid node registration",
			node: types.Node{
				ID:           "test-node-1",
				ExporterType: "node_exporter",
				Port:         9100,
				MetricsPath:  "/metrics",
				Labels:       map[string]string{"env": "prod"},
				Status:       "active",
				LastSeen:     time.Now(),
			},
			groupName:   "test-group",
			ttl:         10 * time.Second,
			expectError: false,
			validate: func(t *testing.T, r *EtcdRegistry, node types.Node) error {
				// Add retry logic for validation
				var lastErr error
				for i := 0; i < 5; i++ { // Try up to 5 times
					nodes, err := r.GetNodes(context.Background(), r.ServicePath("test-group"))
					if err != nil {
						lastErr = fmt.Errorf("validation failed: %v", err)
						time.Sleep(time.Second)
						continue
					}
					if len(nodes) == 1 {
						got := nodes[0]
						if got.ID != node.ID {
							return fmt.Errorf("node ID mismatch: want %s, got %s", node.ID, got.ID)
						}
						if got.Status != "healthy" {
							return fmt.Errorf("node status mismatch: want healthy, got %s", got.Status)
						}
						return nil
					}
					lastErr = fmt.Errorf("expected 1 node, got %d", len(nodes))
					time.Sleep(time.Second)
				}
				return lastErr
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			group, err := r.CreateOrJoinServiceGroup(ctx, tt.groupName)
			if err != nil {
				t.Fatalf("Failed to create/join group: %v", err)
			}

			if tt.groupLabels != nil {
				group.Spec.CommonLabels = tt.groupLabels
			}

			done, errChan, err := group.RegisterNode(ctx, tt.node, tt.ttl)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				if err != nil && tt.errorMsg != "" && !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error containing %q, got %q", tt.errorMsg, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			select {
			case <-done:
				if tt.validate != nil {
					if err := tt.validate(t, r, tt.node); err != nil {
						t.Error(err)
					}
				}
			case err := <-errChan:
				t.Errorf("Registration failed with error: %v", err)
			case <-time.After(5 * time.Second):
				t.Error("Registration timed out")
			}
		})
	}
}

func TestLeaseKeepalive(t *testing.T) {
	r, ctx, cleanup := setupTest(t)
	defer cleanup()

	// Create a test group
	group, err := r.CreateOrJoinServiceGroup(ctx, "keepalive-test-group")
	if err != nil {
		t.Fatalf("Failed to create test group: %v", err)
	}

	// Create a test node with a longer TTL
	testNode := types.Node{
		ID:           "keepalive-test-node",
		ExporterType: "test_exporter",
		Port:         9100,
		MetricsPath:  "/metrics",
		Labels:       map[string]string{"test": "keepalive"},
	}

	ttl := 10 * time.Second
	done, errChan, err := group.RegisterNode(ctx, testNode, ttl)
	if err != nil {
		t.Fatalf("Failed to register node: %v", err)
	}

	// Wait for initial registration
	select {
	case <-done:
		// Success
	case err := <-errChan:
		t.Fatalf("Registration failed: %v", err)
	case <-time.After(time.Second):
		t.Fatal("Registration timed out")
	}

	// Verify node exists initially
	nodes, err := group.GetNodes(ctx)
	if err != nil {
		t.Fatalf("Failed to get nodes: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("Expected 1 node, got %d", len(nodes))
	}

	// Wait longer than the original TTL
	time.Sleep(ttl + 2*time.Second)

	// Verify node still exists due to keepalive
	nodes, err = group.GetNodes(ctx)
	if err != nil {
		t.Fatalf("Failed to get nodes after TTL: %v", err)
	}
	if len(nodes) != 1 {
		t.Errorf("Expected node to still exist due to keepalive, got %d nodes", len(nodes))
	}
}

func TestNodeTTLExpiration(t *testing.T) {
	r, ctx, cleanup := setupTest(t)
	defer cleanup()

	// Create a test group
	group, err := r.CreateOrJoinServiceGroup(ctx, "ttl-test-group")
	if err != nil {
		t.Fatalf("Failed to create test group: %v", err)
	}

	// Create a test node with a short TTL
	testNode := types.Node{
		ID:           "ttl-test-node",
		ExporterType: "test_exporter",
		Port:         9100,
		MetricsPath:  "/metrics",
		Labels:       map[string]string{"test": "ttl"},
	}

	shortTTL := 2 * time.Second
	regCtx, regCancel := context.WithCancel(ctx)
	done, errChan, err := group.RegisterNode(regCtx, testNode, shortTTL)
	if err != nil {
		t.Fatalf("Failed to register node: %v", err)
	}

	// Wait for initial registration
	select {
	case <-done:
		// Success
	case err := <-errChan:
		t.Fatalf("Registration failed: %v", err)
	case <-time.After(time.Second):
		t.Fatal("Registration timed out")
	}

	// Verify node exists
	nodes, err := group.GetNodes(ctx)
	if err != nil {
		t.Fatalf("Failed to get nodes: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("Expected 1 node, got %d", len(nodes))
	}

	// Cancel registration and wait for TTL to expire (TTL + buffer)
	regCancel()
	time.Sleep(shortTTL + 2*time.Second)

	// Verify node has been removed
	nodes, err = group.GetNodes(ctx)
	if err != nil {
		t.Fatalf("Failed to get nodes after TTL: %v", err)
	}
	if len(nodes) != 0 {
		t.Errorf("Expected 0 nodes after TTL expiration, got %d", len(nodes))
	}
}

func TestGetServiceGroups(t *testing.T) {
	r, ctx, cleanup := setupTest(t)
	defer cleanup()

	// Clear any existing data
	if _, err := r.client.Delete(ctx, r.etcdBasePath, clientv3.WithPrefix()); err != nil {
		t.Fatalf("Failed to clear test data: %v", err)
	}

	tests := []struct {
		name           string
		setupGroups    []string
		expectedGroups int
		expectError    bool
		setupNodes     map[string][]types.Node
	}{
		{
			name:           "Multiple groups",
			setupGroups:    []string{"group1", "group2", "group3"},
			expectedGroups: 3,
			setupNodes: map[string][]types.Node{
				"group1": {{ID: "node1", ExporterType: "node_exporter", Port: 9100, MetricsPath: "/metrics"}},
				"group2": {{ID: "node2", ExporterType: "node_exporter", Port: 9101, MetricsPath: "/metrics"}},
				"group3": {{ID: "node3", ExporterType: "node_exporter", Port: 9102, MetricsPath: "/metrics"}},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test data
			for group, nodes := range tt.setupNodes {
				for _, node := range nodes {
					group, err := r.CreateOrJoinServiceGroup(ctx, group)
					if err != nil {
						t.Fatalf("Failed to create/join group: %v", err)
					}
					done, errChan, err := group.RegisterNode(ctx, node, 10*time.Second)
					if err != nil {
						t.Fatalf("Failed to setup test data: %v", err)
					}
					select {
					case <-done:
						// Success
					case err := <-errChan:
						t.Fatalf("Failed to setup test data: %v", err)
					case <-time.After(5 * time.Second):
						t.Fatal("Setup timed out")
					}
				}
			}

			// Test GetServiceGroups
			groups, err := r.GetServiceGroups(ctx)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if len(groups) != tt.expectedGroups {
				t.Errorf("Expected %d groups, got %d", tt.expectedGroups, len(groups))
			}

			// Verify all expected groups are present
			groupMap := make(map[string]bool)
			for _, group := range groups {
				groupMap[group] = true
			}

			for _, expectedGroup := range tt.setupGroups {
				if !groupMap[expectedGroup] {
					t.Errorf("Expected group %s not found", expectedGroup)
				}
			}
		})
	}
}
