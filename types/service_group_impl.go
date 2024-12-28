package types

import (
	"context"
	"encoding/json"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"path"
	"strings"
	"time"
)

// ServiceGroup represents an active service group and its operations
type ServiceGroup struct {
	Registry Registry
	Name     string
	Spec     ServiceGroupSpec
}

// Registry interface defines the methods needed by ServiceGroup
type Registry interface {
	ServicePath(groupName string) string
	NodePath(groupName string, node Node) string
	RegisterNodeWithRetry(ctx context.Context, groupName string, node Node, ttl time.Duration, done chan<- struct{}, errChan chan error)
	DeleteNode(ctx context.Context, groupName string, node Node) error
	GetNodes(ctx context.Context, groupPath string) ([]Node, error)
	GetClient() *clientv3.Client
	GetEtcdBasePath() string
}

// Configure updates the group's configuration both locally and in etcd
func (g *ServiceGroup) Configure(spec ServiceGroupSpec) error {

	// Validate CommonLabels
	if spec.CommonLabels != nil {
		for k, v := range spec.CommonLabels {
			if k == "" {
				return fmt.Errorf("empty label key not allowed in CommonLabels")
			}
			if v == "" {
				return fmt.Errorf("empty label value not allowed for key %q in CommonLabels", k)
			}
			if strings.Contains(k, "=") {
				return fmt.Errorf("label key cannot contain '=' in CommonLabels: %q", k)
			}
		}
	}

	// Create context for etcd operation
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Update etcd first
	groupPath := path.Join(g.Registry.GetEtcdBasePath(), g.Name)

	// Get current group data
	resp, err := g.Registry.GetClient().Get(ctx, groupPath)
	if err != nil {
		return fmt.Errorf("failed to get current group data: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return fmt.Errorf("group %s not found", g.Name)
	}

	// Unmarshal current data
	var data ServiceGroupData
	if err := json.Unmarshal(resp.Kvs[0].Value, &data); err != nil {
		return fmt.Errorf("failed to unmarshal group data: %w", err)
	}

	// Update spec in data
	data.Spec = spec

	// Marshal updated data
	updatedData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal updated group data: %w", err)
	}

	// Perform atomic update
	txn := g.Registry.GetClient().Txn(ctx).If(
		clientv3.Compare(clientv3.ModRevision(groupPath), "=", resp.Kvs[0].ModRevision),
	).Then(
		clientv3.OpPut(groupPath, string(updatedData)),
	)

	txnResp, err := txn.Commit()
	if err != nil {
		return fmt.Errorf("failed to update group data: %w", err)
	}

	if !txnResp.Succeeded {
		return fmt.Errorf("concurrent modification detected, please retry")
	}

	// Only update local state after successful etcd update
	g.Spec = spec
	return nil
}

// RegisterNode registers a new node in this group
func (g *ServiceGroup) RegisterNode(ctx context.Context, node Node, ttl time.Duration) (<-chan struct{}, <-chan error, error) {
	// Apply group's common labels to node
	if node.Labels == nil {
		node.Labels = make(map[string]string)
	}
	for k, v := range g.Spec.CommonLabels {
		if _, exists := node.Labels[k]; !exists {
			node.Labels[k] = v
		}
	}

	// Validate node
	if node.ID == "" {
		return nil, nil, fmt.Errorf("node ID must be non empty")
	}
	if node.ExporterType == "" {
		return nil, nil, fmt.Errorf("exporter type must be non empty")
	}
	if node.Port <= 0 {
		return nil, nil, fmt.Errorf("port must be > 0")
	}
	if node.MetricsPath == "" {
		return nil, nil, fmt.Errorf("metrics path must be non empty")
	}
	if ttl.Seconds() <= 0 {
		return nil, nil, fmt.Errorf("ttl must be > 0")
	}

	// Validate labels
	for k, v := range node.Labels {
		if k == "" {
			return nil, nil, fmt.Errorf("empty label key not allowed")
		}
		if v == "" {
			return nil, nil, fmt.Errorf("empty label value not allowed for key: %s", k)
		}
		if strings.Contains(k, "=") {
			return nil, nil, fmt.Errorf("label key cannot contain '=': %s", k)
		}
	}

	// Update node status
	node.Status = "healthy"
	node.LastSeen = time.Now()

	done := make(chan struct{})
	errChan := make(chan error, 100)

	go g.Registry.RegisterNodeWithRetry(ctx, g.Name, node, ttl, done, errChan)

	return done, errChan, nil
}

// GetNodes returns all nodes in this group
func (g *ServiceGroup) GetNodes(ctx context.Context) ([]Node, error) {
	servicePath := g.Registry.ServicePath(g.Name)
	return g.Registry.GetNodes(ctx, servicePath)
}

// Delete removes the service group and all its nodes
func (g *ServiceGroup) Delete(ctx context.Context) error {
	groupPath := path.Join(g.Registry.GetEtcdBasePath(), g.Name)
	_, err := g.Registry.GetClient().Delete(ctx, groupPath, clientv3.WithPrefix())
	return err
}
