/*
Package etcdregistry provides a library for registering and retrieving service nodes from etcd.
*/
package etcdregistry

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/client/v3"
)

const (
	// Connection management
	maxBackoffDuration = 2 * time.Minute
	initialBackoff     = 1 * time.Second
	backoffFactor      = 2.0
	keepAliveTimeout   = 5 * time.Second // Shorter timeout

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
	var lastErr error
	
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		r.logger.WithFields(logrus.Fields{
			"operation": operation,
			"attempt":   attempt,
		}).Debug("Attempting operation")

		if err := f(); err == nil {
			if attempt > 1 {
				r.logger.WithField("attempts", attempt).Info("Operation succeeded after retries")
			}
			return nil
		} else {
			lastErr = err
			if attempt == maxAttempts {
				break
			}

			backoff := calculateBackoff(attempt)
			r.logger.WithFields(logrus.Fields{
				"operation": operation,
				"attempt":   attempt,
				"backoff":   backoff,
				"error":     err,
			}).Warn("Operation failed, will retry")

			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during %s: %w", operation, ctx.Err())
			case <-time.After(backoff):
				continue
			}
		}
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
	keepAliveCh  <-chan *clientv3.LeaseKeepAliveResponse
}

// Node represents a registered service node.
type Node struct {
	Name string
	Info map[string]string
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
func NewEtcdRegistry(etcdEndpoints []string, etcdBasePath string, etcdUsername string, etcdPassword string, defaultTimeout time.Duration) (*EtcdRegistry, error) {
	if len(etcdEndpoints) == 0 {
		return nil, fmt.Errorf("etcd endpoints cannot be empty")
	}

	logger := logrus.WithFields(logrus.Fields{
		"component": "etcd-registry",
		"basePath":  etcdBasePath,
		"endpoints": strings.Join(etcdEndpoints, ","),
	})

	ctx, cancel := context.WithCancel(context.Background())

	r := &EtcdRegistry{
		defaultTimeout:  defaultTimeout,
		etcdBasePath:    etcdBasePath,
		etcdEndpoints:   etcdEndpoints,
		etcdUsername:    etcdUsername,
		etcdPassword:    etcdPassword,
		logger:          logger,
		ctx:             ctx,
		cancel:          cancel,
		connectionState: stateDisconnected,
		healthTicker:    time.NewTicker(healthCheckInterval),
	}

	// Start health monitoring
	r.wg.Add(1)
	go r.monitorHealth()

	// Initialize connection
	if err := r.connect(); err != nil {
		cancel()
		return nil, fmt.Errorf("failed to initialize connection: %w", err)
	}

	return r, nil
}

// Close gracefully shuts down the registry
func (r *EtcdRegistry) Close() error {
	r.cancel()
	r.healthTicker.Stop()

	// Wait for all goroutines to finish
	r.wg.Wait()

	// Close client connection
	r.clientMutex.Lock()
	defer r.clientMutex.Unlock()

	if r.client != nil {
		if err := r.client.Close(); err != nil {
			return fmt.Errorf("failed to close etcd client: %w", err)
		}
		r.client = nil
	}

	return nil
}

// RegisterNode registers a new Node to a service with a TTL.
// After registration, TTL lease will be kept alive until node is unregistered or process killed.
//
// Args:
//
//	ctx (context.Context): The context for the operation.
//	serviceName (string): The name of the service.
//	node (Node): The node to register.
//	ttl (time.Duration): The TTL for the node.
//
// Returns:
//
//	<-chan struct{}: A channel that is closed when the initial registration is complete.
//	<-chan error: A channel that receives registration errors.
//	error: An error if the initial validation fails.
func (r *EtcdRegistry) RegisterNode(ctx context.Context, serviceName string, node Node, ttl time.Duration) (<-chan struct{}, <-chan error, error) {
	// Input validation
	if serviceName == "" {
		return nil, nil, fmt.Errorf("service name must be non empty")
	}
	if node.Name == "" {
		return nil, nil, fmt.Errorf("node.Name must be non empty")
	}
	if ttl.Seconds() <= 0 {
		return nil, nil, fmt.Errorf("ttl must be > 0")
	}

	logger := r.logger.WithFields(logrus.Fields{
		"service": serviceName,
		"node":    node.Name,
		"ttl":     ttl.String(),
	})

	done := make(chan struct{})
	errChan := make(chan error, 1)

	go func() {
		defer close(errChan)

		var registered bool
		backoff := initialBackoff
		attempts := 0

		for {
			select {
			case <-ctx.Done():
				logger.WithError(ctx.Err()).Info("Registration stopped due to context cancellation")
				errChan <- ctx.Err()
				return
			default:
				attempts++
				logger.WithField("attempt", attempts).Debug("Attempting node registration")

				err := r.registerNode(ctx, serviceName, node, ttl)
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
							logger.Warn("Error channel full, dropping error")
						}
					}

					// Calculate next backoff with exponential increase
					backoff = time.Duration(float64(initialBackoff) * math.Pow(backoffFactor, float64(attempts-1)))
					if backoff > maxBackoffDuration {
						backoff = maxBackoffDuration
					}

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
	}()

	return done, errChan, nil
}

func (r *EtcdRegistry) registerNode(ctx context.Context, serviceName string, node Node, ttl time.Duration) error {
	// Create timeout context for the entire operation
	opCtx, cancel := context.WithTimeout(ctx, r.defaultTimeout)
	defer cancel()

	// Initialize etcd client
	if r.client == nil {
		return fmt.Errorf("client not initialized")
	}

	// Create lease with timeout context
	lease, err := r.client.Grant(opCtx, int64(ttl.Seconds()))
	if err != nil {
		return fmt.Errorf("failed to grant lease: %w", err)
	}

	// Put with lease using timeout context
	nodePath := r.nodePath(serviceName, node.Name)
	nodeData := encode(node.Info)

	_, err = r.client.Put(opCtx, nodePath, nodeData, clientv3.WithLease(lease.ID))
	if err != nil {
		return fmt.Errorf("failed to put node data: %w", err)
	}

	// Start keepalive with parent context (not timeout context)
	keepAlive, err := r.client.KeepAlive(ctx, lease.ID)
	if err != nil {
		return fmt.Errorf("failed to start keepalive: %w", err)
	}

	// Monitor keepalive in separate goroutine
	go func() {
		for range keepAlive {
			// Just consume the channel
			select {
			case <-ctx.Done():
				return
			default:
				continue
			}
		}
	}()

	return nil
}

// GetServiceNodes returns a list of active service nodes.
//
// Args:
//
//	serviceName (string): The name of the service.
//
// Returns:
//
//	[]Node: A list of active service nodes.
//	error: An error if the nodes could not be retrieved.
func (r *EtcdRegistry) GetServiceNodes(serviceName string) ([]Node, error) {
	if r.client == nil {
		return nil, fmt.Errorf("client not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.defaultTimeout)
	defer cancel()

	servicePath := r.servicePath(serviceName)
	rsp, err := r.client.Get(ctx, servicePath+"/", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	nodes := make([]Node, 0)
	for _, n := range rsp.Kvs {
		nodeName := strings.TrimPrefix(string(n.Key), servicePath+"/")
		nodeInfo := decode(n.Value)
		nodes = append(nodes, Node{Name: nodeName, Info: nodeInfo})
	}

	return nodes, nil
}

// initializeETCDClient initializes and validates the etcd client connection.
// It handles both authenticated and non-authenticated connections, and validates
// the connection is working before returning.
func (r *EtcdRegistry) initializeETCDClient() (*clientv3.Client, error) {
	r.clientMutex.RLock()
	if r.client != nil && r.connectionState == stateConnected {
		defer r.clientMutex.RUnlock()
		return r.client, nil
	}
	r.clientMutex.RUnlock()

	if err := r.connect(); err != nil {
		return nil, err
	}

	r.clientMutex.RLock()
	defer r.clientMutex.RUnlock()
	return r.client, nil
}

// encode encodes a map to a JSON string.
func encode(m map[string]string) string {
	if m != nil {
		b, _ := json.Marshal(m)
		return string(b)
	}
	return ""
}

// decode decodes a JSON string to a map.
func decode(ds []byte) map[string]string {
	if ds != nil && len(ds) > 0 {
		var s map[string]string
		err := json.Unmarshal(ds, &s)
		if err != nil {
			logrus.WithError(err).Error("Failed to decode JSON")
			return nil
		}
		return s
	}
	return nil
}

// servicePath returns the path for a service.
func (r *EtcdRegistry) servicePath(serviceName string) string {
	service := strings.Replace(serviceName, "/", "-", -1)
	return path.Join(r.etcdBasePath, service)
}

// nodePath returns the path for a node.
func (r *EtcdRegistry) nodePath(serviceName string, nodeName string) string {
	service := strings.Replace(serviceName, "/", "-", -1)
	node := strings.Replace(nodeName, "/", "-", -1)
	return path.Join(r.etcdBasePath, service, node)
}

// GetServiceGroups returns a list of all service group names under the base path
func (r *EtcdRegistry) GetServiceGroups() ([]string, error) {
	if r.client == nil {
		return nil, fmt.Errorf("client not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), r.defaultTimeout)
	defer cancel()

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

// connect establishes a new connection to etcd
func (r *EtcdRegistry) connect() error {
	var client *clientv3.Client
	
	return r.performWithBackoff(r.ctx, "connect", 5, func() error {
		config := clientv3.Config{
			Endpoints:            r.etcdEndpoints,
			Username:            r.etcdUsername,
			Password:            r.etcdPassword,
			DialTimeout:         30 * time.Second,
			DialKeepAliveTime:   10 * time.Second,
			DialKeepAliveTimeout: 5 * time.Second,
			AutoSyncInterval:    1 * time.Minute,
			PermitWithoutStream: true,
		}

		var err error
		client, err = clientv3.New(config)
		if err != nil {
			return fmt.Errorf("failed to create client: %w", err)
		}

		// Test the connection with a simple operation
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		
		_, err = client.Get(ctx, "test-key")
		if err != nil {
			client.Close()
			return fmt.Errorf("connection test failed: %w", err)
		}

		r.client = client
		r.connectionState = stateConnected
		r.logger.Info("Successfully connected to etcd")
		return nil
	})
}

// monitorHealth continuously monitors connection health
func (r *EtcdRegistry) monitorHealth() {
	defer r.wg.Done()

	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()

	consecutiveFailures := 0
	for {
		select {
		case <-r.ctx.Done():
			return

		case <-ticker.C:
			if err := r.checkHealth(); err != nil {
				consecutiveFailures++
				r.logger.WithError(err).WithField("failures", consecutiveFailures).Warn("Health check failed")

				if consecutiveFailures >= maxFailureCount {
					r.logger.Error("Max failures reached, attempting reconnection")
					if err := r.reconnect(); err != nil {
						r.logger.WithError(err).Error("Reconnection failed")
					} else {
						consecutiveFailures = 0
					}
				}
			} else {
				if consecutiveFailures > 0 {
					r.logger.Info("Health check recovered")
					consecutiveFailures = 0
				}
			}
		}
	}
}

// checkHealth validates the current connection
func (r *EtcdRegistry) checkHealth() error {
	r.clientMutex.RLock()
	defer r.clientMutex.RUnlock()

	if r.client == nil {
		return fmt.Errorf("no active connection")
	}

	ctx, cancel := context.WithTimeout(r.ctx, keepAliveTimeout)
	defer cancel()

	// Try multiple endpoints if available
	var lastErr error
	for _, endpoint := range r.etcdEndpoints {
		if _, err := r.client.Status(ctx, endpoint); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}

	r.failureCount++
	r.lastFailure = time.Now()
	return fmt.Errorf("health check failed on all endpoints: %w", lastErr)
}

// reconnect attempts to reestablish the connection with backoff
func (r *EtcdRegistry) reconnect() error {
	return r.performWithBackoff(r.ctx, "reconnect", 5, func() error {
		return r.connect()
	})
}
