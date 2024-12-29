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
	"math/rand"
	"path"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/client/v3"
	_ "go.lumeweb.com/etcd-registry/types"
	"golang.org/x/time/rate"
)

const (
	// Connection management
	maxBackoffDuration = 2 * time.Minute
	initialBackoff     = 1 * time.Second
	backoffFactor      = 2.0

	// Health check parameters with increased intervals and jitter
	healthCheckInterval  = 5 * time.Minute    // Increased to 5 minutes
	healthCheckJitterMax = 60 * time.Second   // Increased jitter
	maxFailureCount     = 10                  // More tolerant of failures

	// Circuit breaker settings - commented out as we'll disable it
	// circuitBreakerThreshold = 10
	// circuitBreakerTimeout   = 30 * time.Second

	// Lease management - reduced frequency
	maxLeaseOperationsPerSecond = 2                // Reduced rate limit
	leaseReuseWindow            = 10 * time.Minute // Increased reuse window
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
// leaseInfo tracks information about an active lease
type leaseInfo struct {
	id        clientv3.LeaseID
	expiresAt time.Time
	ttl       time.Duration
}

// circuitBreaker implements basic circuit breaker pattern
type circuitBreaker struct {
	failures  int
	lastFail  time.Time
	isOpen    bool
	openUntil time.Time
	mu        sync.RWMutex
}

type EtcdRegistry struct {
	etcdBasePath   string
	etcdUsername   string
	etcdPassword   string
	etcdEndpoints  []string
	defaultTimeout time.Duration
	maxRetries     int
	logger         *logrus.Entry

	// Track working endpoints
	workingEndpoints   []string
	workingEndpointMu sync.RWMutex

	// Lease management
	leaseCache   map[string]*leaseInfo // key -> lease info
	leaseCacheMu sync.RWMutex

	// Circuit breaker
	circuitBreaker *circuitBreaker

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

	// Rate limiting
	leaseCleanupLimiter *rate.Limiter
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
		defaultTimeout:      defaultTimeout,
		etcdBasePath:        etcdBasePath,
		etcdEndpoints:       etcdEndpoints,
		etcdUsername:        etcdUsername,
		etcdPassword:        etcdPassword,
		maxRetries:          maxRetries,
		connectionState:     stateDisconnected,
		leaseCleanupLimiter: rate.NewLimiter(rate.Limit(maxLeaseOperationsPerSecond), 1),
		leaseCache:          make(map[string]*leaseInfo),
		circuitBreaker:      &circuitBreaker{},
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

	// Start health monitoring with jitter
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()

		// Create ticker with jitter
		jitter := time.Duration(rand.Int63n(int64(healthCheckJitterMax)))
		ticker := time.NewTicker(healthCheckInterval + jitter)
		defer ticker.Stop()

		for {
			select {
			case <-r.ctx.Done():
				return
			case <-ticker.C:
				if err := r.checkHealth(); err != nil {
					r.circuitBreaker.recordFailure()
				} else {
					r.circuitBreaker.reset()
				}
			}
		}
	}()

	return r, nil
}

// Close gracefully shuts down the registry and cleans up resources
func (r *EtcdRegistry) Close() error {
	r.cancel()
	r.healthTicker.Stop()

	// Wait for all goroutines to finish
	r.wg.Wait()

	// Skip lease cleanup entirely during shutdown
	// Just clean up the lease cache
	r.leaseCacheMu.Lock()
	r.leaseCache = make(map[string]*leaseInfo)
	r.leaseCacheMu.Unlock()

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
	var currentLease clientv3.LeaseID

	cleanup := func() {
		if currentLease != 0 {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := r.leaseCleanupLimiter.Wait(cleanupCtx); err == nil {
				_, _ = r.client.Lease.Revoke(cleanupCtx, currentLease)
			}
		}
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

			// Simple wait before retry
			select {
			case <-ctx.Done():
				return
			case <-time.After(ttl / 2):
				continue
			}
		}
	}
}

func (r *EtcdRegistry) registerNode(ctx context.Context, serviceName string, node types.Node, ttl time.Duration) error {
	// Check circuit breaker
	if !r.circuitBreaker.canTry() {
		return fmt.Errorf("circuit breaker is open")
	}

	// Check client state
	if r.client == nil {
		return fmt.Errorf("etcd client connection unavailable")
	}

	// Try to reuse existing lease
	nodePath := r.NodePath(serviceName, node)
	cacheKey := fmt.Sprintf("%s/%s", serviceName, node.ID)

	r.leaseCacheMu.RLock()
	existingLease, exists := r.leaseCache[cacheKey]
	r.leaseCacheMu.RUnlock()

	if exists && time.Now().Before(existingLease.expiresAt) {
		// Verify lease is still valid
		ttlResp, err := r.client.TimeToLive(ctx, existingLease.id)
		if err == nil && ttlResp.TTL > 0 {
			// Reuse existing lease
			nodeData, err := json.Marshal(node)
			if err != nil {
				return fmt.Errorf("failed to marshal node data: %w", err)
			}

			_, err = r.client.Put(ctx, nodePath, string(nodeData), clientv3.WithLease(existingLease.id))
			if err == nil {
				r.logger.WithFields(logrus.Fields{
					"node":     node.ID,
					"leaseID": existingLease.id,
				}).Debug("Reusing existing lease")
				return nil
			}
		}
		// If verification failed, remove from cache
		r.leaseCacheMu.Lock()
		delete(r.leaseCache, cacheKey)
		r.leaseCacheMu.Unlock()
	}

	// Disabled aggressive lease cleanup
	// resp, err := r.client.Get(ctx, nodePath)
	// if err == nil && len(resp.Kvs) > 0 {
	//	if lease := resp.Kvs[0].Lease; lease != 0 {
	//		if err := r.leaseCleanupLimiter.Wait(ctx); err == nil {
	//			_, _ = r.client.Lease.Revoke(ctx, clientv3.LeaseID(lease))
	//		}
	//	}
	// }

	// Rate limit new lease creation
	if err := r.leaseCleanupLimiter.Wait(ctx); err != nil {
		return fmt.Errorf("lease creation rate limit exceeded: %w", err)
	}

	// Create new lease
	leaseResp, err := r.client.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		if r.circuitBreaker.recordFailure() {
			r.logger.Warn("Circuit breaker opened due to lease creation failures")
		}
		return fmt.Errorf("failed to grant lease: %w", err)
	}

	// Cache the new lease
	r.leaseCacheMu.Lock()
	r.leaseCache[cacheKey] = &leaseInfo{
		id:        leaseResp.ID,
		expiresAt: time.Now().Add(ttl),
		ttl:       ttl,
	}
	r.leaseCacheMu.Unlock()

	// Put with lease using timeout context
	nodeData, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("failed to marshal node data: %w", err)
	}

	_, err = r.client.Put(ctx, nodePath, string(nodeData), clientv3.WithLease(leaseResp.ID))
	if err != nil {
		// Skip lease cleanup on failed put, let it expire naturally
		return fmt.Errorf("failed to put node data: %w", err)
	}

	// Start keepalive with parent context
	keepAlive, err := r.client.KeepAlive(ctx, leaseResp.ID)
	if err != nil {
		// Skip lease cleanup on failed keepalive, let it expire naturally
		return fmt.Errorf("failed to start keepalive: %w", err)
	}

	r.logger.WithFields(logrus.Fields{
		"node":     node.ID,
		"leaseID": leaseResp.ID,
	}).Debug("Created new lease")

	// Monitor keepalive responses in a goroutine
	go func() {
		defer func() {
			if err := r.DeleteNode(ctx, serviceName, node); err != nil {
				r.logger.WithError(err).Error("Failed to cleanup node")
			}
		}()

		for {
			select {
			case ka, ok := <-keepAlive:
				if !ok {
					// Channel closed - lease expired or connection lost
					r.logger.WithField("node_id", node.ID).Info("Keepalive channel closed")
					return
				}
				if ka != nil {
					r.logger.WithFields(logrus.Fields{
						"node_id":  node.ID,
						"lease_id": ka.ID,
						"ttl":      ka.TTL,
					}).Debug("Keepalive successful")
				}
			case <-ctx.Done():
				r.logger.WithField("node_id", node.ID).Info("Registration context cancelled")
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
		MaxCallSendMsgSize:   0, // Use default
		MaxCallRecvMsgSize:   0, // Use default
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

// recordFailure records a failure in the circuit breaker
func (cb *circuitBreaker) recordFailure() bool {
	return false  // Temporarily disable circuit breaker
}

// canTry checks if the circuit breaker allows an operation
func (cb *circuitBreaker) canTry() bool {
	return true  // Temporarily disable circuit breaker
}

// reset resets the circuit breaker state
func (cb *circuitBreaker) reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures = 0
	cb.isOpen = false
	cb.lastFail = time.Time{}
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

	if r.client == nil {
		r.connectionState = stateDisconnected
		return fmt.Errorf("no active connection")
	}

	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()

	// Just try one endpoint
	for _, endpoint := range r.etcdEndpoints {
		if _, err := r.client.Status(ctx, endpoint); err == nil {
			r.connectionState = stateConnected
			return nil
		}
	}

	r.connectionState = stateDisconnected
	return fmt.Errorf("health check failed on all endpoints")
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

// nodesEqualExceptLastSeen compares two nodes ignoring the LastSeen field
func nodesEqualExceptLastSeen(a, b types.Node) bool {
	// Create copies to avoid modifying original nodes
	aCopy := a
	bCopy := b

	// Zero out LastSeen fields
	aCopy.LastSeen = time.Time{}
	bCopy.LastSeen = time.Time{}

	return reflect.DeepEqual(aCopy, bCopy)
}

// WatchServices watches for all service and node changes
func (r *EtcdRegistry) WatchServices(ctx context.Context) (<-chan types.WatchEvent, error) {
	if r.client == nil {
		return nil, fmt.Errorf("etcd client connection unavailable")
	}

	eventChan := make(chan types.WatchEvent)

	go func() {
		defer close(eventChan)

		watcher := r.client.Watch(ctx, r.etcdBasePath, clientv3.WithPrefix())

		for {
			select {
			case <-ctx.Done():
				return

			case watchResponse := <-watcher:
				if watchResponse.Err() != nil {
					r.logger.WithError(watchResponse.Err()).Error("Watch error occurred")
					return
				}

				for _, event := range watchResponse.Events {
					key := string(event.Kv.Key)
					relativePath := strings.TrimPrefix(key, r.etcdBasePath+"/")
					parts := strings.Split(relativePath, "/")

					if len(parts) < 2 {
						continue
					}

					groupName := parts[0]
					var eventType types.WatchEventType

					switch event.Type {
					case clientv3.EventTypePut:
						if event.IsCreate() {
							eventType = types.EventTypeCreate
						} else {
							eventType = types.EventTypeUpdate
						}
					case clientv3.EventTypeDelete:
						eventType = types.EventTypeDelete
					}

					if len(parts) > 2 && parts[1] == "nodes" {
						var node types.Node
						if event.Type != clientv3.EventTypeDelete {
							if err := json.Unmarshal(event.Kv.Value, &node); err != nil {
								r.logger.WithError(err).Error("Failed to unmarshal node data")
								continue
							}
						}

						select {
						case eventChan <- types.WatchEvent{
							Type:      eventType,
							GroupName: groupName,
							Node:      &node,
						}:
						case <-ctx.Done():
							return
						}
					} else {
						select {
						case eventChan <- types.WatchEvent{
							Type:      eventType,
							GroupName: groupName,
						}:
						case <-ctx.Done():
							return
						}
					}
				}
			}
		}
	}()

	return eventChan, nil
}

// WatchGroup watches for changes in a specific service group
func (r *EtcdRegistry) WatchGroup(ctx context.Context, groupName string) (<-chan types.WatchEvent, error) {
	if r.client == nil {
		return nil, fmt.Errorf("etcd client connection unavailable")
	}

	groupPath := path.Join(r.etcdBasePath, groupName)
	eventChan := make(chan types.WatchEvent)

	go func() {
		defer close(eventChan)

		watcher := r.client.Watch(ctx, groupPath, clientv3.WithPrefix())

		for {
			select {
			case <-ctx.Done():
				return

			case watchResponse := <-watcher:
				if watchResponse.Err() != nil {
					r.logger.WithError(watchResponse.Err()).Error("Watch error occurred")
					return
				}

				for _, event := range watchResponse.Events {
					var eventType types.WatchEventType

					switch event.Type {
					case clientv3.EventTypePut:
						if event.IsCreate() {
							eventType = types.EventTypeCreate
						} else {
							eventType = types.EventTypeUpdate
						}
					case clientv3.EventTypeDelete:
						eventType = types.EventTypeDelete
					}

					select {
					case eventChan <- types.WatchEvent{
						Type:      eventType,
						GroupName: groupName,
					}:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return eventChan, nil
}

// WatchGroupNodes watches for node changes in a specific service group
func (r *EtcdRegistry) WatchGroupNodes(ctx context.Context, groupName string) (<-chan types.WatchEvent, error) {
	if r.client == nil {
		return nil, fmt.Errorf("etcd client connection unavailable")
	}

	nodesPath := r.ServicePath(groupName)
	eventChan := make(chan types.WatchEvent)

	go func() {
		defer close(eventChan)

		watcher := r.client.Watch(ctx, nodesPath, clientv3.WithPrefix())

		for {
			select {
			case <-ctx.Done():
				return

			case watchResponse := <-watcher:
				if watchResponse.Err() != nil {
					r.logger.WithError(watchResponse.Err()).Error("Watch error occurred")
					return
				}

				for _, event := range watchResponse.Events {
					var eventType types.WatchEventType

					switch event.Type {
					case clientv3.EventTypePut:
						if event.IsCreate() {
							eventType = types.EventTypeCreate
						} else {
							eventType = types.EventTypeUpdate
						}
					case clientv3.EventTypeDelete:
						eventType = types.EventTypeDelete
					}

					if event.Type != clientv3.EventTypeDelete {
						var newNode types.Node
						if err := json.Unmarshal(event.Kv.Value, &newNode); err != nil {
							r.logger.WithError(err).Error("Failed to unmarshal node data")
							continue
						}

						// For updates, check if only LastSeen changed
						if eventType == types.EventTypeUpdate && event.PrevKv != nil {
							var oldNode types.Node
							if err := json.Unmarshal(event.PrevKv.Value, &oldNode); err != nil {
								r.logger.WithError(err).Error("Failed to unmarshal previous node data")
								continue
							}

							// Compare nodes ignoring LastSeen field
							if nodesEqualExceptLastSeen(oldNode, newNode) {
								r.logger.WithField("node", newNode.ID).Debug("Ignoring update event - only LastSeen changed")
								continue
							}
						}

						select {
						case eventChan <- types.WatchEvent{
							Type:      eventType,
							GroupName: groupName,
							Node:      &newNode,
						}:
						case <-ctx.Done():
							return
						}
					}
				}
			}
		}
	}()

	return eventChan, nil
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

	// Create a separate context with timeout for the delete operation
	deleteCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nodePath := r.NodePath(groupName, node)
	resp, err := r.client.Delete(deleteCtx, nodePath)
	if err != nil {
		return fmt.Errorf("failed to delete node: %w", err)
	}

	// If no keys were deleted, the node was already gone - that's okay
	if resp.Deleted == 0 {
		r.logger.WithField("path", nodePath).Debug("Node already deleted")
		return nil
	}

	// Skip group cleanup if original context is cancelled
	select {
	case <-ctx.Done():
		return nil
	default:
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()

		// Best-effort group cleanup
		group, err := r.GetServiceGroup(cleanupCtx, groupName)
		if err != nil {
			r.logger.WithError(err).WithField("group", groupName).Debug("Skipping group cleanup")
			return nil
		}

		nodes, err := group.GetNodes(cleanupCtx)
		if err != nil {
			r.logger.WithError(err).WithField("group", groupName).Debug("Skipping group cleanup")
			return nil
		}

		if len(nodes) == 0 {
			groupPath := path.Join(r.etcdBasePath, groupName)
			if _, err := r.client.Delete(cleanupCtx, groupPath, clientv3.WithPrefix()); err != nil {
				r.logger.WithError(err).WithField("group", groupName).Debug("Failed to cleanup empty group")
			} else {
				r.logger.WithField("group", groupName).Debug("Cleaned up empty group")
			}
		}
	}

	return nil
}
