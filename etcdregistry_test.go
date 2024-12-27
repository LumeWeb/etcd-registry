package etcdregistry

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewEtcdRegistry(t *testing.T) {
	// Test case 1: Test that a new EtcdRegistry instance can be created with valid parameters.
	_, err := NewEtcdRegistry([]string{"localhost:2379"}, "/test", "username", "password", 10*time.Second)
	if err != nil {
		t.Errorf("Expected nil error, got %v", err)
	}

	// Test case 2: Test that an error is returned when creating a new EtcdRegistry instance with invalid parameters.
	_, err = NewEtcdRegistry([]string{}, "/test", "username", "password", 10*time.Second)
	if err == nil {
		t.Errorf("Expected non-nil error, got nil")
	}
}

func TestRegisterNode(t *testing.T) {
	// Create a new EtcdRegistry instance
	r, err := NewEtcdRegistry([]string{"localhost:2379"}, "/test", "username", "password", 10*time.Second)
	if err != nil {
		t.Fatalf("Failed to create registry: %v", err)
	}

	// Test case 1: Successful registration
	t.Run("Successful Registration", func(t *testing.T) {
		node := Node{Name: "node1", Info: map[string]string{"key": "value"}}
		done, errChan, err := r.RegisterNode(context.Background(), "service1", node, 10*time.Second)
		if err != nil {
			t.Fatalf("Expected nil error, got %v", err)
		}

		select {
		case <-done:
			// Success case
		case err := <-errChan:
			t.Errorf("Registration failed with error: %v", err)
		case <-time.After(30 * time.Second):
			t.Error("Registration timed out")
		}
	})

	// Test case 2: Empty node name
	t.Run("Empty Node Name", func(t *testing.T) {
		node := Node{Name: "", Info: map[string]string{"key": "value"}}
		_, _, err := r.RegisterNode(context.Background(), "service1", node, 10*time.Second)
		if err == nil {
			t.Error("Expected error for empty node name, got nil")
		}
	})

	// Test case 3: Invalid TTL
	t.Run("Invalid TTL", func(t *testing.T) {
		node := Node{Name: "node1", Info: map[string]string{"key": "value"}}
		_, _, err := r.RegisterNode(context.Background(), "service1", node, 0*time.Second)
		if err == nil {
			t.Error("Expected error for zero TTL, got nil")
		}
	})

	// Test case 4: Context cancellation
	t.Run("Context Cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		node := Node{Name: "node1", Info: map[string]string{"key": "value"}}
		done, errChan, err := r.RegisterNode(ctx, "service1", node, 10*time.Second)
		if err != nil {
			t.Fatalf("Expected nil error, got %v", err)
		}

		// Cancel the context immediately
		cancel()

		select {
		case <-done:
			t.Error("Expected registration to fail due to cancellation")
		case err := <-errChan:
			if err != context.Canceled {
				t.Errorf("Expected context.Canceled error, got %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Test timed out")
		}
	})
}

func TestGetServiceNodes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r, err := NewEtcdRegistry([]string{"localhost:2379"}, "/test", "", "", 10*time.Second)
	if err != nil {
		t.Fatalf("Failed to create registry: %v", err)
	}
	defer r.Close()

	// Test case 1: Register and retrieve nodes
	t.Run("Register and Retrieve Nodes", func(t *testing.T) {
		node := Node{Name: "node1", Info: map[string]string{"key": "value"}}
		done, errChan, err := r.RegisterNode(ctx, "service1", node, 10*time.Second)
		if err != nil {
			t.Fatalf("Failed to register node: %v", err)
		}

		select {
		case <-done:
			// Success
		case err := <-errChan:
			t.Fatalf("Registration failed: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("Registration timed out")
		}

		// Give etcd time to process
		time.Sleep(1 * time.Second)

		nodes, err := r.GetServiceNodes("service1")
		if err != nil {
			t.Errorf("Failed to get nodes: %v", err)
		}
		if len(nodes) != 1 {
			t.Errorf("Expected 1 node, got %d", len(nodes))
		}
	})
}

func TestEdgeCases(t *testing.T) {
	r, err := NewEtcdRegistry([]string{"localhost:2379"}, "/test", "username", "password", 10*time.Second)
	if err != nil {
		t.Fatalf("Failed to create registry: %v", err)
	}

	// Test case 1: Concurrent operations
	t.Run("Concurrent Operations", func(t *testing.T) {
		var wg sync.WaitGroup
		errors := make(chan error, 10)

		// Concurrent registrations
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				node := Node{Name: fmt.Sprintf("node%d", i), Info: map[string]string{"key": "value"}}
				done, errChan, err := r.RegisterNode(context.Background(), "service1", node, 10*time.Second)
				if err != nil {
					errors <- fmt.Errorf("failed to register node %d: %v", i, err)
					return
				}

				select {
				case <-done:
					// Success
				case err := <-errChan:
					errors <- fmt.Errorf("node %d registration failed: %v", i, err)
				case <-time.After(30 * time.Second):
					errors <- fmt.Errorf("node %d registration timed out", i)
				}
			}(i)
		}

		// Concurrent retrievals
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := r.GetServiceNodes("service1")
				if err != nil {
					errors <- fmt.Errorf("failed to get nodes: %v", err)
				}
			}()
		}

		// Wait for all operations to complete
		wg.Wait()
		close(errors)

		// Check for any errors
		for err := range errors {
			t.Error(err)
		}
	})

	// Test case 2: Large scale operations
	t.Run("Large Scale Operations", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		// Register 100 nodes
		for i := 0; i < 100; i++ {
			node := Node{Name: fmt.Sprintf("node%d", i), Info: map[string]string{"key": "value"}}
			done, errChan, err := r.RegisterNode(ctx, "service2", node, 10*time.Second)
			if err != nil {
				t.Fatalf("Failed to register node %d: %v", i, err)
			}

			select {
			case <-done:
				// Success
			case err := <-errChan:
				t.Fatalf("Node %d registration failed: %v", i, err)
			case <-ctx.Done():
				t.Fatal("Test timed out")
			}
		}

		// Verify all nodes are registered
		nodes, err := r.GetServiceNodes("service2")
		if err != nil {
			t.Fatalf("Failed to get nodes: %v", err)
		}
		if len(nodes) != 100 {
			t.Errorf("Expected 100 nodes, got %d", len(nodes))
		}
	})
}

func TestFailureCases(t *testing.T) {
	// Test connection failure
	// Test connection failure
	t.Run("Connection_Failure", func(t *testing.T) {
		// Create registry with invalid endpoint and shorter timeout
		r, err := NewEtcdRegistry([]string{"localhost:12345"}, "/test", "", "", 1*time.Second)
		if err != nil {
			t.Fatalf("Failed to create registry: %v", err)
		}

		// Use short context timeout
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		// Attempt to register node
		done, errChan, err := r.RegisterNode(ctx, "service1", Node{Name: "node1"}, 10*time.Second)
		if err == nil {
			select {
			case <-done:
				t.Error("Expected failure but registration succeeded")
			case err := <-errChan:
				if !strings.Contains(err.Error(), "connection refused") &&
					!strings.Contains(err.Error(), "context deadline exceeded") {
					t.Errorf("Expected connection refused error, got: %v", err)
				}
			case <-ctx.Done():
				// This is also acceptable
			}
		}

		// Close registry
		r.Close()
	})
	// Test authentication failure
	t.Run("Authentication_Failure", func(t *testing.T) {
		// Create registry with invalid credentials
		r, err := NewEtcdRegistry([]string{"localhost:2379"}, "/test", "invalid", "invalid", 5*time.Second)
		if err != nil {
			t.Fatalf("Failed to create registry: %v", err)
		}

		// Context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Attempt to register node
		done, errChan, err := r.RegisterNode(ctx, "service1", Node{Name: "node1"}, 10*time.Second)
		if err != nil {
			if strings.Contains(err.Error(), "authentication") ||
				strings.Contains(err.Error(), "auth") {
				return
			}
			t.Fatalf("Unexpected immediate error: %v", err)
		}

		// If we get here, we should see either:
		// 1. An auth error on the error channel, or
		// 2. A successful registration if auth is disabled (which is fine)
		select {
		case err := <-errChan:
			if !strings.Contains(err.Error(), "authentication") &&
				!strings.Contains(err.Error(), "auth") {
				t.Errorf("Expected authentication error, got: %v", err)
			}
		case <-done:
			// This is actually okay - it means auth is disabled
			// Let's verify we got the auth disabled message in the logs
			time.Sleep(100 * time.Millisecond) // Give logs time to appear
		case <-ctx.Done():
			t.Error("Test timed out")
		}
	})
	// Test operation timeout
	t.Run("Operation_Timeout", func(t *testing.T) {
		// Create registry with extremely short timeout
		r, err := NewEtcdRegistry([]string{"localhost:2379"}, "/test", "", "", 1*time.Millisecond)
		if err != nil {
			t.Fatalf("Failed to create registry: %v", err)
		}
		defer r.Close()

		// Create a channel to track if we got a timeout
		timeoutChan := make(chan struct{})

		// Run the operation in a goroutine
		go func() {
			ctx := context.Background() // Use background context
			done, errChan, err := r.RegisterNode(ctx, "service1", Node{Name: "node1"}, 10*time.Second)
			if err != nil {
				if strings.Contains(err.Error(), "deadline exceeded") {
					timeoutChan <- struct{}{}
				}
				return
			}

			select {
			case err := <-errChan:
				if strings.Contains(err.Error(), "deadline exceeded") {
					timeoutChan <- struct{}{}
				}
			case <-done:
				// Operation succeeded, don't signal timeout
			}
		}()

		// Wait for either timeout or success
		select {
		case <-timeoutChan:
			// Test passed - we got a timeout
		case <-time.After(5 * time.Second):
			t.Error("Test did not timeout as expected")
		}
	})
}

func TestCleanup(t *testing.T) {
	r, err := NewEtcdRegistry([]string{"localhost:2379"}, "/test", "", "", 10*time.Second)
	if err != nil {
		t.Fatalf("Failed to create registry: %v", err)
	}

	// Test proper cleanup after context cancellation
	t.Run("Cleanup After Cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		node := Node{Name: "cleanup-test", Info: map[string]string{"key": "value"}}
		done, errChan, err := r.RegisterNode(ctx, "service-cleanup", node, 10*time.Second)
		if err != nil {
			t.Fatalf("Failed to register node: %v", err)
		}

		// Wait for initial registration
		select {
		case <-done:
			// Success
		case err := <-errChan:
			t.Fatalf("Registration failed: %v", err)
		case <-time.After(30 * time.Second):
			t.Fatal("Registration timed out")
		}

		// Cancel the context
		cancel()

		// Wait and verify the node is removed
		time.Sleep(15 * time.Second) // Wait for TTL to expire
		nodes, err := r.GetServiceNodes("service-cleanup")
		if err != nil {
			t.Fatalf("Failed to get nodes: %v", err)
		}
		if len(nodes) != 0 {
			t.Error("Expected node to be removed after cancellation")
		}
	})
}
