/*
Package etcdregistry provides a library for registering and retrieving service nodes from etcd.
*/
package etcdregistry

import (
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
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
	connectionCooldown = 30 * time.Second
	defaultInitTimeout = 2 * time.Minute
	keepAliveTime      = 10 * time.Second // More frequent keepalives
	keepAliveTimeout   = 5 * time.Second  // Shorter timeout
	dialTimeout        = 20 * time.Second // Longer dial timeout

	// Health check parameters
	healthCheckInterval = 5 * time.Second // More frequent health checks
	maxFailureCount     = 5               // More failures allowed before reconnect
	reconnectDelay      = 2 * time.Second // Shorter reconnect delay
	maxRetryAttempts    = 5               // Max retry attempts for operations

	// GRPC settings
	grpcBackoffMultiplier = 1.6
	grpcBackoffMaxDelay   = 120 * time.Second
	grpcMinConnectTimeout = 20 * time.Second

	// Connection states
	stateDisconnected = "disconnected"
	stateConnecting   = "connecting"
	stateConnected    = "connected"
	stateError        = "error"
)

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
	logger := r.logger.WithFields(logrus.Fields{
		"service": serviceName,
		"node":    node.Name,
		"ttl":     ttl.String(),
	})

	// Initialize etcd client
	cli, err := r.initializeETCDClient()
	if err != nil {
		return fmt.Errorf("failed to initialize etcd client: %w", err)
	}
	defer func(cli *clientv3.Client) {
		err := cli.Close()
		if err != nil {
			logger.WithError(err).Error("Failed to close etcd client")
		}
	}(cli)

	// Create lease
	leaseCtx, leaseCancel := context.WithTimeout(ctx, r.defaultTimeout)
	defer leaseCancel()

	lease, err := cli.Grant(leaseCtx, int64(ttl.Seconds()))
	if err != nil {
		return fmt.Errorf("failed to grant lease: %w", err)
	}

	logger.WithField("lease_id", lease.ID).Debug("Lease granted")

	// Register node
	putCtx, putCancel := context.WithTimeout(ctx, r.defaultTimeout)
	defer putCancel()

	nodePath := r.nodePath(serviceName, node.Name)
	nodeData := encode(node.Info)

	_, err = cli.Put(putCtx, nodePath, nodeData, clientv3.WithLease(lease.ID))
	if err != nil {
		return fmt.Errorf("failed to put node data: %w", err)
	}

	logger.WithFields(logrus.Fields{
		"path": nodePath,
		"data": nodeData,
	}).Debug("Node data written to etcd")

	// Start keepalive
	keepAlive, err := cli.KeepAlive(ctx, lease.ID)
	if err != nil {
		return fmt.Errorf("failed to start keepalive: %w", err)
	}

	// Monitor keepalive in separate goroutine
	go func() {
		keepAliveLogger := logger.WithField("lease_id", lease.ID)
		keepAliveLogger.Info("Starting keepalive monitoring")

		for {
			select {
			case resp, ok := <-keepAlive:
				if !ok {
					keepAliveLogger.Warn("Keepalive channel closed")
					return
				}
				keepAliveLogger.WithFields(logrus.Fields{
					"ttl": resp.TTL,
				}).Debug("Received keepalive response")
			case <-ctx.Done():
				keepAliveLogger.WithError(ctx.Err()).Info("Keepalive monitoring stopped due to context cancellation")
				return
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
	logger := r.logger.WithField("service", serviceName)

	if serviceName == "" {
		return nil, fmt.Errorf("service name cannot be empty")
	}

	cli, err := r.initializeETCDClient()
	if err != nil {
		logger.WithError(err).Error("Failed to initialize etcd client")
		return nil, err
	}
	defer func(cli *clientv3.Client) {
		err := cli.Close()
		if err != nil {
			logger.WithError(err).Error("Failed to close etcd client")
		}
	}(cli)

	ctx, cancel := context.WithTimeout(context.Background(), r.defaultTimeout)
	defer cancel()

	servicePath := r.servicePath(serviceName)
	logger.WithField("path", servicePath).Debug("Fetching service nodes")

	rsp, err := cli.Get(ctx, servicePath+"/", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if err != nil {
		logger.WithError(err).Error("Failed to get service nodes")
		return nil, err
	}

	nodes := make([]Node, 0)

	if len(rsp.Kvs) == 0 {
		logger.Debug("No service nodes found")
		return nodes, nil
	}

	for _, n := range rsp.Kvs {
		nodeName := strings.TrimPrefix(string(n.Key), servicePath+"/")
		nodeInfo := decode(n.Value)

		node := Node{
			Name: nodeName,
			Info: nodeInfo,
		}
		nodes = append(nodes, node)

		logger.WithFields(logrus.Fields{
			"node": nodeName,
			"info": nodeInfo,
		}).Debug("Found service node")
	}

	logger.WithField("count", len(nodes)).Info("Successfully retrieved service nodes")
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

// connect establishes a new connection to etcd
func (r *EtcdRegistry) connect() error {
	r.clientMutex.Lock()
	defer r.clientMutex.Unlock()

	if r.connectionState == stateConnecting {
		return fmt.Errorf("connection already in progress")
	}

	r.connectionState = stateConnecting
	r.logger.Info("Establishing connection to etcd")

	// Create connection parameters for stability
	connParams := grpc.ConnectParams{
		Backoff: backoff.Config{
			BaseDelay:  1.0 * time.Second,
			Multiplier: 1.6,
			Jitter:     0.2,
			MaxDelay:   120 * time.Second,
		},
		MinConnectTimeout: 20 * time.Second,
	}

	// Create GRPC options for connection stability
	grpcOpts := []grpc.DialOption{
		grpc.WithConnectParams(connParams),
		grpc.WithReturnConnectionError(),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	}

	config := clientv3.Config{
		Endpoints:            r.etcdEndpoints,
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepAliveTime,
		DialKeepAliveTimeout: keepAliveTimeout,
		DialOptions:          grpcOpts,
		Context:              r.ctx,
		RejectOldCluster:     true,
		AutoSyncInterval:     5 * time.Minute,
		PermitWithoutStream:  true,
	}

	if r.etcdUsername != "" && r.etcdPassword != "" {
		config.Username = r.etcdUsername
		config.Password = r.etcdPassword
	}

	// Create client with retry
	var client *clientv3.Client
	var err error
	for attempt := 0; attempt < maxRetryAttempts; attempt++ {
		if attempt > 0 {
			r.logger.WithField("attempt", attempt+1).Info("Retrying connection")
			time.Sleep(time.Duration(attempt) * time.Second)
		}

		client, err = clientv3.New(config)
		if err == nil {
			break
		}
		r.logger.WithError(err).WithField("attempt", attempt+1).Warn("Connection attempt failed")
	}

	if err != nil {
		r.connectionState = stateError
		r.failureCount++
		r.lastFailure = time.Now()
		return fmt.Errorf("failed to create etcd client after %d attempts: %w", maxRetryAttempts, err)
	}

	// Validate connection with retry
	ctx, cancel := context.WithTimeout(r.ctx, r.defaultTimeout)
	defer cancel()

	for attempt := 0; attempt < maxRetryAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}

		if _, err := client.Status(ctx, r.etcdEndpoints[0]); err == nil {
			break
		} else if attempt == maxRetryAttempts-1 {
			r.connectionState = stateError
			r.failureCount++
			r.lastFailure = time.Now()
			client.Close()
			return fmt.Errorf("failed to validate connection after %d attempts: %w", maxRetryAttempts, err)
		}
	}

	// Update client reference
	if r.client != nil {
		if err := r.client.Close(); err != nil {
			r.logger.WithError(err).Warn("Error closing old client")
		}
	}

	r.client = client
	r.connectionState = stateConnected
	r.failureCount = 0

	r.logger.Info("Successfully connected to etcd")
	return nil
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
	backoff := initialBackoff
	maxAttempts := 5

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		r.logger.WithField("attempt", attempt).Info("Attempting reconnection")

		if err := r.connect(); err != nil {
			if attempt == maxAttempts {
				return fmt.Errorf("failed to reconnect after %d attempts: %w", maxAttempts, err)
			}

			backoff = time.Duration(float64(initialBackoff) * math.Pow(backoffFactor, float64(attempt-1)))
			if backoff > maxBackoffDuration {
				backoff = maxBackoffDuration
			}

			r.logger.WithFields(logrus.Fields{
				"attempt": attempt,
				"backoff": backoff,
				"error":   err,
			}).Warn("Reconnection attempt failed")

			select {
			case <-r.ctx.Done():
				return fmt.Errorf("context cancelled during reconnection")
			case <-time.After(backoff):
				continue
			}
		}

		r.logger.Info("Reconnection successful")
		return nil
	}

	return fmt.Errorf("reconnection failed after %d attempts", maxAttempts)
}
