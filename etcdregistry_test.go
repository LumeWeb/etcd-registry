package etcdregistry

import (
	"strings"
	"testing"
	"time"
)

func TestNewEtcdRegistry(t *testing.T) {
	tests := []struct {
		name        string
		endpoints   []string
		basePath    string
		username    string
		password    string
		timeout     time.Duration
		maxRetries  int
		expectError bool
		errorMsg    string
	}{
		{
			name:        "Valid parameters without auth",
			endpoints:   []string{"localhost:2379"},
			basePath:    "/test",
			username:    "",
			password:    "",
			timeout:     10 * time.Second,
			maxRetries:  3,
			expectError: false,
		},
		{
			name:        "Valid parameters with auth",
			endpoints:   []string{"localhost:2379"},
			basePath:    "/test",
			username:    "username",
			password:    "password",
			timeout:     1 * time.Second,
			maxRetries:  0,
			expectError: true,
			errorMsg:    "authentication is not enabled",
		},
		{
			name:        "Empty endpoints",
			endpoints:   []string{},
			basePath:    "/test",
			username:    "username",
			password:    "password",
			timeout:     10 * time.Second,
			maxRetries:  3,
			expectError: true,
			errorMsg:    "etcd endpoints cannot be empty",
		},
		{
			name:        "Empty base path",
			endpoints:   []string{"localhost:2379"},
			basePath:    "",
			username:    "username",
			password:    "password",
			timeout:     10 * time.Second,
			maxRetries:  3,
			expectError: true,
			errorMsg:    "etcd base path cannot be empty",
		},
		{
			name:        "Invalid endpoint",
			endpoints:   []string{"invalid:2379"},
			basePath:    "/test",
			username:    "username",
			password:    "password",
			timeout:     100 * time.Millisecond,
			maxRetries:  1,
			expectError: true,
			errorMsg:    "failed to initialize connection",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry, err := NewEtcdRegistry(tt.endpoints, tt.basePath, tt.username, tt.password, tt.timeout, tt.maxRetries)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error, got nil")
				} else if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tt.errorMsg)) {
					t.Errorf("Expected error containing %q, got %q", tt.errorMsg, err.Error())
				}
				if registry != nil {
					t.Error("Expected nil registry when error occurs")
				}
				return
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if registry == nil {
					t.Error("Expected non-nil registry")
				}
			}

			if registry != nil {
				if err := registry.Close(); err != nil {
					t.Fatalf("Failed to close registry: %v", err)
				}
			}
		})
	}
}
