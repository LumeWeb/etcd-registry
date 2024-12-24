/*
Package etcdregistry provides a library for registering and retrieving service nodes from etcd.
*/
package etcdregistry

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/client/v3"
)

// EtcdRegistry is a library for registering and retrieving service nodes from etcd.
type EtcdRegistry struct {
	// etcdBasePath is the base path for all etcd operations.
	etcdBasePath string
	// etcdUsername is the username for etcd authentication.
	etcdUsername string
	// etcdPassword is the password for etcd authentication.
	etcdPassword string
	// etcdEndpoints are the endpoints for the etcd cluster.
	etcdEndpoints []string
	// defaultTimeout is the default timeout for etcd operations.
	defaultTimeout time.Duration
}

// Node represents a registered service node.
type Node struct {
	// Name is the name of the node.
	Name string
	// Info is a map of additional information about the node.
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
	r := &EtcdRegistry{}
	r.defaultTimeout = defaultTimeout
	r.etcdBasePath = etcdBasePath
	r.etcdEndpoints = etcdEndpoints
	r.etcdUsername = etcdUsername
	r.etcdPassword = etcdPassword
	return r, nil
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

	// Create channels for signaling
	done := make(chan struct{})
	errChan := make(chan error, 1) // Buffered channel to prevent blocking

	// Start the registration process in a goroutine
	go func() {
		defer close(errChan) // Ensure channel is closed when goroutine exits

		var registered bool
		backoff := time.Second

		for {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
				err := r.registerNode(ctx, serviceName, node, ttl)
				if err != nil {
					logrus.Errorf("Failed to register node %s/%s: %v", serviceName, node.Name, err)

					// Send error to error channel if not already registered
					if !registered {
						select {
						case errChan <- err:
						default:
							// Channel full, log and continue
							logrus.Warn("Error channel full, dropping error")
						}
					}

					// Exponential backoff with max delay of 1 minute
					if backoff < time.Minute {
						backoff *= 2
					}

					select {
					case <-ctx.Done():
						return
					case <-time.After(backoff):
						continue
					}
				}

				// Successful registration
				if !registered {
					registered = true
					close(done)
					backoff = time.Second // Reset backoff on successful registration
				}

				// Wait before next registration attempt
				select {
				case <-ctx.Done():
					return
				case <-time.After(ttl / 2): // Refresh at half the TTL interval
					continue
				}
			}
		}
	}()

	return done, errChan, nil
}

// keepRegistered keeps the node registered by periodically re-registering it.
func (r *EtcdRegistry) keepRegistered(ctx context.Context, serviceName string, node Node, ttl time.Duration, done chan struct{}) {
	for {
		// Register the node.
		err := r.registerNode(ctx, serviceName, node, ttl)
		if err != nil {
			logrus.Warnf("Registration stopped. Retrying. err=%s", err)
			time.Sleep(5 * time.Second)
			continue
		}
		// Close the done channel to signal that the initial registration is complete
		close(done)
		logrus.Infof("Registration on ETCD done. Retrying.")
		time.Sleep(10 * time.Second)
	}
}

// registerNode handles a single registration attempt
func (r *EtcdRegistry) registerNode(ctx context.Context, serviceName string, node Node, ttl time.Duration) error {
	// Initialize etcd client
	cli, err := r.initializeETCDClient()
	if err != nil {
		return fmt.Errorf("failed to initialize etcd client: %w", err)
	}
	defer cli.Close()

	// Create lease
	leaseCtx, leaseCancel := context.WithTimeout(ctx, r.defaultTimeout)
	defer leaseCancel()

	lease, err := cli.Grant(leaseCtx, int64(ttl.Seconds()))
	if err != nil {
		return fmt.Errorf("failed to grant lease: %w", err)
	}

	// Register node
	putCtx, putCancel := context.WithTimeout(ctx, r.defaultTimeout)
	defer putCancel()

	_, err = cli.Put(putCtx, r.nodePath(serviceName, node.Name), encode(node.Info), clientv3.WithLease(lease.ID))
	if err != nil {
		return fmt.Errorf("failed to put node data: %w", err)
	}

	// Start keepalive
	keepAlive, err := cli.KeepAlive(ctx, lease.ID)
	if err != nil {
		return fmt.Errorf("failed to start keepalive: %w", err)
	}

	// Monitor keepalive in separate goroutine
	go func() {
		for {
			select {
			case resp, ok := <-keepAlive:
				if !ok {
					logrus.Warnf("Keepalive channel closed for %s/%s", serviceName, node.Name)
					return
				}
				logrus.Debugf("Received keepalive response for %s/%s: TTL=%d", serviceName, node.Name, resp.TTL)
			case <-ctx.Done():
				logrus.Infof("Context cancelled for keepalive of %s/%s", serviceName, node.Name)
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
	// Check if the service name is empty.
	if serviceName == "" {
		return nil, fmt.Errorf("service name cannot be empty")
	}

	// Initialize the etcd client.
	cli, err := r.initializeETCDClient()
	if err != nil {
		return nil, err
	}

	// Get the service nodes.
	rsp, err := cli.Get(context.Background(), r.servicePath(serviceName)+"/", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if err != nil {
		return nil, err
	}

	// Create a list of nodes.
	nodes := make([]Node, 0)

	// Check if there are any service nodes.
	if len(rsp.Kvs) == 0 {
		logrus.Debugf("No services nodes were found under %s", r.servicePath(serviceName)+"/")
		return nodes, nil
	}

	// Iterate over the service nodes.
	for _, n := range rsp.Kvs {
		// Create a new node.
		node := Node{}
		node.Name = strings.TrimPrefix(string(n.Key), r.servicePath(serviceName)+"/")
		node.Info = decode(n.Value)
		nodes = append(nodes, node)
	}

	return nodes, nil
}

// initializeETCDClient initializes and validates the etcd client connection.
// It handles both authenticated and non-authenticated connections, and validates
// the connection is working before returning.
func (r *EtcdRegistry) initializeETCDClient() (*clientv3.Client, error) {
	// Validate endpoints
	if len(r.etcdEndpoints) == 0 {
		return nil, fmt.Errorf("no etcd endpoints provided")
	}

	// Create context with timeout for connection
	ctx, cancel := context.WithTimeout(context.Background(), r.defaultTimeout)
	defer cancel()

	// Build base config
	config := clientv3.Config{
		Endpoints:   r.etcdEndpoints,
		DialTimeout: r.defaultTimeout,
		Context:     ctx,
	}

	// Add authentication only if both username and password are provided
	if r.etcdUsername != "" && r.etcdPassword != "" {
		config.Username = r.etcdUsername
		config.Password = r.etcdPassword
	}

	// Initialize client
	cli, err := clientv3.New(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	// Ensure connection is cleaned up if subsequent operations fail
	success := false
	defer func() {
		if !success {
			cli.Close()
		}
	}()

	// Test connection by getting cluster status
	statusCtx, statusCancel := context.WithTimeout(ctx, r.defaultTimeout)
	defer statusCancel()

	_, err = cli.Status(statusCtx, r.etcdEndpoints[0])
	if err != nil {
		// Special handling for authentication errors
		if strings.Contains(err.Error(), "authentication is not enabled") {
			// If server has auth disabled but we provided credentials, retry without auth
			if r.etcdUsername != "" || r.etcdPassword != "" {
				logrus.Debug("Authentication is disabled on server, retrying without credentials")
				r.etcdUsername = ""
				r.etcdPassword = ""
				return r.initializeETCDClient()
			}
		}
		return nil, fmt.Errorf("failed to connect to etcd: %w", err)
	}

	// Verify we can access the base path
	checkCtx, checkCancel := context.WithTimeout(ctx, r.defaultTimeout)
	defer checkCancel()

	// Try to read from base path to verify permissions
	_, err = cli.Get(checkCtx, r.etcdBasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to access base path '%s': %w", r.etcdBasePath, err)
	}

	// Connection is good
	success = true
	logrus.Debugf("Successfully connected to etcd cluster at %v with base path '%s'",
		r.etcdEndpoints, r.etcdBasePath)

	return cli, nil
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
		json.Unmarshal(ds, &s)
		return s
	}
	return nil
}

// servicePath returns the path for a service.
func (r *EtcdRegistry) servicePath(serviceName string) string {
	// Replace '/' with '-' in the service name.
	service := strings.Replace(serviceName, "/", "-", -1)
	return path.Join(r.etcdBasePath, service)
}

// nodePath returns the path for a node.
func (r *EtcdRegistry) nodePath(serviceName string, nodeName string) string {
	// Replace '/' with '-' in the service name and node name.
	service := strings.Replace(serviceName, "/", "-", -1)
	node := strings.Replace(nodeName, "/", "-", -1)
	return path.Join(r.etcdBasePath, service, node)
}
