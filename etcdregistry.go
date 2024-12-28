/*
Package etcdregistry provides a library for registering and retrieving service nodes from etcd.
*/
package etcdregistry

import (
	"context"
	"encoding/json"
	"fmt"
	"go.lumeweb.com/etcd-registry/types"
	"math"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/client/v3"
	_ "go.lumeweb.com/etcd-registry/types"
)

const (
	// Connection management
	maxBackoffDuration = 2 * time.Minute
	initialBackoff     = 1 * time.Second
	backoffFactor      = 2.0

	// Health check parameters
	healthCheckInterval = 5 * time.Second // More frequent health checks
	maxFailureCount     = 5               // More failures allowed before reconnect
	// Connection states
	stateDisconnected = "disconnected"
	stateConnected    = "connected"
)

// calculateBackoff computes the next backoff duration using exponential backoff
func calculateBackoff(attempt int) time.Duration {
	backoff := time.Duration(float64(initialBackoff) * math.Pow(backoffFactor, float64(attempt-1)))
	if backoff > maxBackoffDuration {
		backoff = maxBackoffDuration
	}
	return backoff
}

// performWithBackoff executes an operation with exponential backoff retry logic
func (r *EtcdRegistry) performWithBackoff(ctx context.Context, operation string, maxAttempts int, f func() error) error {
	// Initialize logger safely
	logger := logrus.NewEntry(logrus.StandardLogger())
	if r != nil && r.logger != nil {
		logger = r.logger
	}

	// Validate inputs
	if ctx == nil {
		return fmt.Errorf("nil context provided")
	}
	if f == nil {
		return fmt.Errorf("nil operation function provided")
	}
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("context cancelled during %s: %w (last error: %v)", operation, ctx.Err(), lastErr)
			}
			return fmt.Errorf("context cancelled during %s: %w", operation, ctx.Err())
		default:
			if err := f(); err == nil {
				if attempt > 1 {
					logger.WithField("attempts", attempt).Info("Operation succeeded after retries")
				}
				return nil
			} else {
				lastErr = err
				if attempt == maxAttempts {
					break
				}

				backoff := calculateBackoff(attempt)
				logger.WithFields(logrus.Fields{
					"operation": operation,
					"attempt":   attempt,
					"error":     err,
				}).Debug("Retrying operation")

				timer := time.NewTimer(backoff)
				select {
				case <-ctx.Done():
					timer.Stop()
					return fmt.Errorf("context cancelled during %s backoff: %w (last error: %v)", 
						operation, ctx.Err(), lastErr)
				case <-timer.C:
					continue
				}
			}
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("operation failed without specific error")
	}
	return fmt.Errorf("%s failed after %d attempts: %w", operation, maxAttempts, lastErr)
}

// EtcdRegistry is a library for registering and retrieving service nodes from etcd.
type EtcdRegistry struct {
	etcdBasePath   string
	etcdUsername   string
	etcdPassword   string
	etcdEndpoints  []string
	defaultTimeout time.Duration
	maxRetries     int
	logger         *logrus.Entry

	// Connection management
	client          *clientv3.Client
	clientMutex     sync.RWMutex
	connectionState string
	failureCount    int
	lastFailure     time.Time

	// Lifecycle management
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Health monitoring
	healthTicker *time.Ticker
}

// NewEtcdRegistry creates a new EtcdRegistry instance.
//
// Args:
//
//	etcdEndpoints ([]string): The endpoints for the etcd cluster.
//	etcdBasePath (string): The base path for all etcd operations.
//	etcdUsername (string): The username for etcd authentication.
//	etcdPassword (string): The password for etcd authentication.
//	defaultTimeout (time.Duration): The default timeout for etcd operations.
//
// Returns:
//
//	*EtcdRegistry: A new EtcdRegistry instance.
//	error: An error if the instance could not be created.
func NewEtcdRegistry(etcdEndpoints []string, etcdBasePath string, etcdUsername string, etcdPassword string, defaultTimeout time.Duration, maxRetries int) (*EtcdRegistry, error) {
	initCtx, initCancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer initCancel()
	if len(etcdEndpoints) == 0 {
		return nil, fmt.Errorf("etcd endpoints cannot be empty")
	}

	if strings.TrimSpace(etcdBasePath) == "" {
		return nil, fmt.Errorf("etcd base path cannot be empty")
	}

	if defaultTimeout <= 0*time.Second {
		return nil, fmt.Errorf("default timeout must be greater than zero seconds")
	}

	// Create the registry instance first
	r := &EtcdRegistry{
		logger: logrus.WithFields(logrus.Fields{
			"component": "etcd-registry",
			"basePath":  etcdBasePath,
			"endpoints": strings.Join(etcdEndpoints, ","),
		}),
		defaultTimeout:  defaultTimeout,
		etcdBasePath:    etcdBasePath,
		etcdEndpoints:   etcdEndpoints,
		etcdUsername:    etcdUsername,
		etcdPassword:    etcdPassword,
		maxRetries:      maxRetries,
		connectionState: stateDisconnected,
	}

	// Try to connect first, before creating any goroutines or resources
	if err := r.connect(initCtx); err != nil {
		return nil, fmt.Errorf("failed to initialize connection: %w", err)
	}

	// Only after successful connection, initialize the rest
	ctx, cancel := context.WithCancel(context.Background())
	r.ctx = ctx
	r.cancel = cancel
	r.healthTicker = time.NewTicker(healthCheckInterval)

	// Start health monitoring
	r.wg.Add(1)
	go r.monitorHealth()

	return r, nil
}

// Close gracefully shuts down the registry and cleans up resources
func (r *EtcdRegistry) Close() error {
	r.cancel()
	r.healthTicker.Stop()

	// Wait for all goroutines to finish
	r.wg.Wait()

	// Clean up any lingering leases
	if r.client != nil {
		ctx := context.Background()

		// Best effort cleanup - don't fail if this errors
		if leases, err := r.client.Lease.Leases(ctx); err == nil {
			for _, lease := range leases.Leases {
				_, err := r.client.Lease.Revoke(ctx, lease.ID)
				if err != nil {
					r.logger.WithError(err).Error("Failed to revoke lease")
				}
			}
		}
	}

	// Close client connection
	r.clientMutex.Lock()
	if r.client == nil {
		r.clientMutex.Unlock()
		return nil
	}

	err := r.client.Close()
	r.client = nil
	r.clientMutex.Unlock()

	if err != nil {
		return fmt.Errorf("failed to close etcd client: %w", err)
	}
	return nil
}

// RegisterNodeWithRetry handles the node registration retry loop
func (r *EtcdRegistry) RegisterNodeWithRetry(ctx context.Context, groupName string, node types.Node, ttl time.Duration, done chan<- struct{}, errChan chan error) {
	defer close(errChan)

	logger := r.logger.WithFields(logrus.Fields{
		"service": groupName,
		"node":    node.ID,
		"ttl":     ttl.String(),
	})

	var registered bool
	backoff := initialBackoff
	attempts := 0

	cleanup := func() {
		if err := r.DeleteNode(ctx, groupName, node); err != nil {
			logger.WithError(err).Error("Failed to cleanup node on shutdown")
			select {
			case errChan <- fmt.Errorf("cleanup failed: %w", err):
			default:
				logger.Warn("Error channel full, dropping cleanup error")
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			logger.WithError(ctx.Err()).Info("Registration stopped due to context cancellation")
			cleanup()
			return
		default:
			attempts++
			logger.WithField("attempt", attempts).Debug("Attempting node registration")

			err := r.registerNode(ctx, groupName, node, ttl)
			if err != nil {
				logger.WithFields(logrus.Fields{
					"attempt": attempts,
					"backoff": backoff.String(),
					"error":   err,
				}).Warn("Failed to register node")

				if !registered {
					select {
					case errChan <- err:
					default:
						select {
						case <-errChan:
							errChan <- err
						default:
							logger.Warn("Error channel full, dropping error")
						}
					}
				}

				backoff = calculateBackoff(attempts)

				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
					continue
				}
			}

			if !registered {
				registered = true
				close(done)
				logger.Info("Initial registration successful")
				backoff = initialBackoff
				attempts = 0
			}

			refreshInterval := ttl / 2
			logger.WithField("refresh_interval", refreshInterval).Debug("Waiting before next registration attempt")

			select {
			case <-ctx.Done():
				return
			case <-time.After(refreshInterval):
				continue
			}
		}
	}
}

func (r *EtcdRegistry) registerNode(ctx context.Context, serviceName string, node types.Node, ttl time.Duration) error {
	// Check client state
	if r.client == nil {
		return fmt.Errorf("etcd client connection unavailable")
	}

	// Create lease with timeout context
	lease, err := r.client.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return fmt.Errorf("failed to grant lease: %w", err)
	}

	// Put with lease using timeout context
	nodePath := r.NodePath(serviceName, node)
	nodeData, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("failed to marshal node data: %w", err)
	}

	_, err = r.client.Put(ctx, nodePath, string(nodeData), clientv3.WithLease(lease.ID))
	if err != nil {
		return fmt.Errorf("failed to put node data: %w", err)
	}

	// Start keepalive with parent context (not timeout context)
	keepAlive, err := r.client.KeepAlive(ctx, lease.ID)
	if err != nil {
		return fmt.Errorf("failed to start keepalive: %w", err)
	}

	// Monitor keepalive responses in a goroutine
	go func() {
		defer func() {
			r.logger.WithFields(logrus.Fields{
				"node_id": node.ID,
				"status":  node.Status,
			}).Debug("Registration lease expired/completed")
		}()

		for {
			select {
			case ka, ok := <-keepAlive:
				if !ok {
					// Channel closed - lease expired or connection lost
					return
				}
				if ka == nil {
					// Keepalive failed
					return
				}
				// Successful keepalive response
				r.logger.WithFields(logrus.Fields{
					"node_id":      node.ID,
					"status":       node.Status,
					"lease_id":     ka.ID,
					"ttl":          ka.TTL,
				}).Debug("Keepalive successful")
			case <-ctx.Done():
				// Context cancelled
				return
			}
		}
	}()

	return nil
}

// ServicePath returns the path for a service group's nodes
func (r *EtcdRegistry) ServicePath(groupName string) string {
	return path.Join(r.etcdBasePath, groupName, "nodes")
}

// NodePath returns the path for a specific exporter node
func (r *EtcdRegistry) NodePath(groupName string, node types.Node) string {
	nodeKey := fmt.Sprintf("%s_%s", node.ID, node.ExporterType)
	return path.Join(r.ServicePath(groupName), nodeKey)
}

// CreateOrJoinServiceGroup ensures a group exists and returns a ServiceGroup instance
func (r *EtcdRegistry) CreateOrJoinServiceGroup(ctx context.Context, groupName string) (*types.ServiceGroup, error) {
	if groupName == "" {
		return nil, fmt.Errorf("group name cannot be empty")
	}
	if strings.ContainsAny(groupName, "/\\?#[] ") {
		return nil, fmt.Errorf("invalid group name: must not contain spaces or special characters /\\?#[]")
	}
	if len(groupName) > 63 {
		return nil, fmt.Errorf("group name must be 63 characters or less")
	}
	if !strings.ContainsAny(groupName, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_") {
		return nil, fmt.Errorf("group name must contain only alphanumeric characters, hyphens and underscores")
	}

	groupPath := path.Join(r.etcdBasePath, groupName)

	// Initial group configuration
	data := types.ServiceGroupData{
		Name:    groupName,
		Created: time.Now(),
		Spec: types.ServiceGroupSpec{
			CommonLabels: make(map[string]string),
		},
	}

	initialData, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal initial group data: %w", err)
	}

	// Atomic creation transaction
	txn := r.client.Txn(ctx).If(
		clientv3.Compare(clientv3.CreateRevision(groupPath), "=", 0),
	).Then(
		clientv3.OpPut(groupPath, string(initialData)),
	).Else(
		clientv3.OpGet(groupPath),
	)

	resp, err := txn.Commit()
	if err != nil {
		return nil, fmt.Errorf("failed to create/join group: %w", err)
	}

	// If group already existed, get its current spec
	var spec types.ServiceGroupSpec
	if !resp.Succeeded && len(resp.Responses) > 0 && len(resp.Responses[0].GetResponseRange().Kvs) > 0 {
		var existingData types.ServiceGroupData
		if err := json.Unmarshal(resp.Responses[0].GetResponseRange().Kvs[0].Value, &existingData); err != nil {
			return nil, fmt.Errorf("failed to unmarshal existing group data: %w", err)
		}
		spec = existingData.Spec
	} else {
		spec = data.Spec
	}

	return &types.ServiceGroup{
		Registry: r,
		Name:     groupName,
		Spec:     spec,
	}, nil
}

// UpdateServiceGroupConfig updates group configuration using CAS operations
func (r *EtcdRegistry) UpdateServiceGroupConfig(ctx context.Context, groupName string, updateFn func(*types.ServiceGroup) error) error {
	groupPath := path.Join(r.etcdBasePath, groupName)

	for {
		// Get current value and revision
		resp, err := r.client.Get(ctx, groupPath)
		if err != nil {
			return fmt.Errorf("failed to get group config: %w", err)
		}
		if len(resp.Kvs) == 0 {
			return fmt.Errorf("group %s not found", groupName)
		}

		// Parse current config
		var data types.ServiceGroupData
		if err := json.Unmarshal(resp.Kvs[0].Value, &data); err != nil {
			return fmt.Errorf("failed to unmarshal group config: %w", err)
		}

		group := &types.ServiceGroup{
			Registry: r,
			Name:     groupName,
			Spec:     data.Spec,
		}

		// Apply updates
		if err := updateFn(group); err != nil { // Configure() no longer takes ctx
			return fmt.Errorf("update function failed: %w", err)
		}

		// Update the data with new spec
		data.Spec = group.Spec

		// Marshal updated config
		updatedData, err := json.Marshal(data)
		if err != nil {
			return fmt.Errorf("failed to marshal updated config: %w", err)
		}

		// Attempt atomic update
		txn := r.client.Txn(ctx).If(
			clientv3.Compare(clientv3.ModRevision(groupPath), "=", resp.Kvs[0].ModRevision),
		).Then(
			clientv3.OpPut(groupPath, string(updatedData)),
		)

		txnResp, err := txn.Commit()
		if err != nil {
			return fmt.Errorf("failed to update config: %w", err)
		}

		if txnResp.Succeeded {
			return nil
		}
		// If update failed, retry with new revision
		continue
	}
}

// GetServiceGroup retrieves group configuration
func (r *EtcdRegistry) GetServiceGroup(ctx context.Context, groupName string) (*types.ServiceGroup, error) {
	groupPath := path.Join(r.etcdBasePath, groupName)

	resp, err := r.client.Get(ctx, groupPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get group: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("group %s not found", groupName)
	}

	var data types.ServiceGroupData
	if err := json.Unmarshal(resp.Kvs[0].Value, &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal group data: %w", err)
	}

	// Initialize CommonLabels if nil
	if data.Spec.CommonLabels == nil {
		data.Spec.CommonLabels = make(map[string]string)
	}

	return &types.ServiceGroup{
		Registry: r,
		Name:     groupName,
		Spec:     data.Spec,
	}, nil
}

// GetServiceGroups returns a list of all service group names under the base path
func (r *EtcdRegistry) GetServiceGroups(ctx context.Context) ([]string, error) {
	if r.client == nil {
		return nil, fmt.Errorf("client not initialized")
	}

	// Get all keys under base path
	rsp, err := r.client.Get(ctx, r.etcdBasePath, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return nil, fmt.Errorf("failed to get services: %w", err)
	}

	// Use a map to deduplicate service names
	serviceMap := make(map[string]struct{})

	// Extract unique service names from paths
	for _, kv := range rsp.Kvs {
		relativePath := strings.TrimPrefix(string(kv.Key), r.etcdBasePath+"/")
		parts := strings.Split(relativePath, "/")
		if len(parts) > 0 {
			serviceMap[parts[0]] = struct{}{}
		}
	}

	// Convert map keys to slice
	services := make([]string, 0, len(serviceMap))
	for service := range serviceMap {
		services = append(services, service)
	}

	return services, nil
}

// getClientConfig returns the etcd client configuration
func (r *EtcdRegistry) getClientConfig() clientv3.Config {
	return clientv3.Config{
		Endpoints:            r.etcdEndpoints,
		Username:             r.etcdUsername,
		Password:             r.etcdPassword,
		DialTimeout:          r.defaultTimeout,
		DialKeepAliveTime:    r.defaultTimeout,
		DialKeepAliveTimeout: r.defaultTimeout / 2,
		AutoSyncInterval:     5 * time.Minute,
		PermitWithoutStream:  true,
		MaxCallSendMsgSize:   0,     // Use default
		MaxCallRecvMsgSize:   0,     // Use default
	}
}

// connect establishes a new connection to etcd
func (r *EtcdRegistry) connect(ctx context.Context) error {
	var client *clientv3.Client
	var err error

	connectFn := func() error {
		client, err = clientv3.New(r.getClientConfig())
		if err != nil {
			return fmt.Errorf("failed to create client: %w", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// If credentials are provided, verify auth is enabled
		if r.etcdUsername != "" || r.etcdPassword != "" {
			authStatus, err := client.AuthStatus(ctx)
			if err != nil {
				if cerr := client.Close(); cerr != nil {
					r.logger.WithError(cerr).Error("Failed to close client after error")
				}
				return fmt.Errorf("failed to check auth status: %w", err)
			}
			if !authStatus.Enabled {
				if cerr := client.Close(); cerr != nil {
					r.logger.WithError(cerr).Error("Failed to close client after error")
				}
				return fmt.Errorf("authentication is not enabled")
			}
		}

		// Test connection
		if _, err = client.Get(ctx, "test-key"); err != nil {
			if cerr := client.Close(); cerr != nil {
				r.logger.WithError(cerr).Error("Failed to close client after error")
			}
			return fmt.Errorf("connection test failed: %w", err)
		}

		return nil
	}

	retries := r.maxRetries
	if retries == 0 {
		retries = 1
	}

	if err = r.performWithBackoff(ctx, "connect", retries, connectFn); err != nil {
		return err
	}

	r.clientMutex.Lock()
	r.client = client
	r.connectionState = stateConnected
	r.failureCount = 0
	r.lastFailure = time.Time{}
	r.clientMutex.Unlock()

	r.logger.Info("Successfully connected to etcd")
	return nil
}

// monitorHealth continuously monitors connection health
func (r *EtcdRegistry) monitorHealth() {
	defer func() {
		r.logger.Debug("Health monitoring stopped")
		r.wg.Done()
	}()

	for {
		select {
		case <-r.ctx.Done():
			return

		case <-r.healthTicker.C:
			if err := r.checkHealth(); err != nil {
				r.clientMutex.Lock()
				r.failureCount++
				r.lastFailure = time.Now()
				r.clientMutex.Unlock()

				r.logger.WithError(err).WithField("failures", r.failureCount).Warn("Health check failed")

				if r.failureCount >= maxFailureCount {
					r.logger.Error("Max failures reached, attempting reconnection")
					if err := r.reconnect(); err != nil {
						r.logger.WithError(err).Error("Reconnection failed")
					} else {
						r.clientMutex.Lock()
						r.failureCount = 0
						r.lastFailure = time.Time{}
						r.clientMutex.Unlock()
						r.logger.Info("Successfully reconnected")
					}
				}
			} else {
				r.clientMutex.Lock()
				wasDisconnected := r.failureCount > 0
				r.failureCount = 0
				r.lastFailure = time.Time{}
				r.connectionState = stateConnected
				r.clientMutex.Unlock()

				if wasDisconnected {
					r.logger.Info("Health check recovered")
				}
			}
		}
	}
}

// checkHealth validates the current connection
func (r *EtcdRegistry) checkHealth() error {
	r.clientMutex.Lock()
	defer r.clientMutex.Unlock()

	logger := r.logger.WithFields(logrus.Fields{
		"state":    r.connectionState,
		"failures": r.failureCount,
	})

	if r.client == nil {
		r.connectionState = stateDisconnected
		logger.Debug("Health check failed: nil client")
		return fmt.Errorf("no active connection")
	}

	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()

	// Try multiple endpoints if available
	var lastErr error
	for _, endpoint := range r.etcdEndpoints {
		if _, err := r.client.Status(ctx, endpoint); err == nil {
			// Update state on success
			r.failureCount = 0
			r.lastFailure = time.Time{}
			r.connectionState = stateConnected
			logger.WithField("endpoint", endpoint).Debug("Health check succeeded")
			return nil
		} else {
			lastErr = err
			logger.WithFields(logrus.Fields{
				"endpoint": endpoint,
				"error":    err,
			}).Debug("Endpoint health check failed")
		}
	}

	// Update state on failure
	r.failureCount++
	r.lastFailure = time.Now()
	r.connectionState = stateDisconnected

	logger.WithError(lastErr).Debug("Health check failed on all endpoints")
	return fmt.Errorf("health check failed on all endpoints: %w", lastErr)
}

// reconnect attempts to reestablish the connection with backoff
func (r *EtcdRegistry) reconnect() error {
	return r.performWithBackoff(r.ctx, "reconnect", 5, func() error {
		return r.connect(r.ctx)
	})
}

// GetNodes returns all nodes at the given path
func (r *EtcdRegistry) GetNodes(ctx context.Context, servicePath string) ([]types.Node, error) {
	if r.client == nil {
		return nil, fmt.Errorf("client not initialized")
	}

	rsp, err := r.client.Get(ctx, servicePath, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	nodes := make([]types.Node, 0)
	for _, kv := range rsp.Kvs {
		var node types.Node
		if err := json.Unmarshal(kv.Value, &node); err != nil {
			r.logger.WithError(err).Error("Failed to unmarshal node data")
			continue
		}
		nodes = append(nodes, node)
	}

	return nodes, nil
}

// GetClient returns the etcd client instance
func (r *EtcdRegistry) GetClient() *clientv3.Client {
	return r.client
}

// GetEtcdBasePath returns the base path for etcd operations
func (r *EtcdRegistry) GetEtcdBasePath() string {
	return r.etcdBasePath
}

// DeleteNode removes a node from etcd
func (r *EtcdRegistry) DeleteNode(ctx context.Context, groupName string, node types.Node) error {
	if r.client == nil {
		return fmt.Errorf("etcd client connection unavailable")
	}

	nodePath := r.NodePath(groupName, node)
	resp, err := r.client.Delete(ctx, nodePath)
	if err != nil {
		return fmt.Errorf("failed to delete node: %w", err)
	}

	// Verify deletion was successful
	if resp.Deleted == 0 {
		return fmt.Errorf("node not found at path: %s", nodePath)
	}

	// Check if group is empty and clean it up if needed
	group, err := r.GetServiceGroup(ctx, groupName)
	if err != nil {
		r.logger.WithError(err).WithField("group", groupName).Warn("Failed to get group after deletion")
		return nil
	}

	nodes, err := group.GetNodes(ctx)
	if err != nil {
		r.logger.WithError(err).WithField("group", groupName).Warn("Failed to check group nodes after deletion")
		return nil
	}

	if len(nodes) == 0 {
		groupPath := path.Join(r.etcdBasePath, groupName)
		if _, err := r.client.Delete(ctx, groupPath, clientv3.WithPrefix()); err != nil {
			r.logger.WithError(err).WithField("group", groupName).Warn("Failed to cleanup empty group")
		} else {
			r.logger.WithField("group", groupName).Info("Cleaned up empty group")
		}
	}

	return nil
}
