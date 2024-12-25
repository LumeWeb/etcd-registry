# ETCD Registry

A Go library for service registration and discovery using etcd. This library provides a simple interface for registering services and retrieving service node information from an etcd cluster.

## Features

- Service node registration with TTL (Time To Live)
- Automatic registration renewal
- Service node discovery
- Authentication support
- Configurable timeouts
- Error handling and automatic retries
- Graceful cleanup on context cancellation

## Installation

```bash
go get go.lumeweb.com/etcd-registry
```

## Usage

### Creating a Registry Instance

```go
import (
    "time"
    etcdregistry "go.lumeweb.com/etcd-registry"
)

registry, err := etcdregistry.NewEtcdRegistry(
    []string{"localhost:2379"},  // etcd endpoints
    "/services",                 // base path
    "username",                  // username (optional)
    "password",                  // password (optional)
    10*time.Second,             // default timeout
)
if err != nil {
    log.Fatal(err)
}
```

### Registering a Service Node

```go
node := etcdregistry.Node{
    Name: "node1",
    Info: map[string]string{
        "version": "1.0.0",
        "address": "192.168.1.100:8080",
    },
}

// Register with 30-second TTL
done, errChan, err := registry.RegisterNode(
    context.Background(),
    "my-service",
    node,
    30*time.Second,
)
if err != nil {
    log.Fatal(err)
}

// Wait for initial registration
select {
case <-done:
    log.Println("Node registered successfully")
case err := <-errChan:
    log.Printf("Registration failed: %v", err)
}
```

### Discovering Service Nodes

```go
nodes, err := registry.GetServiceNodes("my-service")
if err != nil {
    log.Fatal(err)
}

for _, node := range nodes {
    fmt.Printf("Node: %s, Info: %v\n", node.Name, node.Info)
}
```

## Configuration

The registry can be configured with the following parameters:

- `etcdEndpoints`: List of etcd server endpoints (required)
- `etcdBasePath`: Base path for all etcd operations (required)
- `etcdUsername`: Username for etcd authentication (optional)
- `etcdPassword`: Password for etcd authentication (optional)
- `defaultTimeout`: Default timeout for etcd operations (required)

## Features in Detail

### Automatic Registration Renewal

The library automatically maintains the registration by:
- Refreshing the TTL periodically (at half the TTL interval)
- Handling connection failures with exponential backoff
- Retrying failed registrations automatically

### Error Handling

The library provides comprehensive error handling:
- Connection failures
- Authentication errors
- Timeout handling
- Context cancellation

### Cleanup

The library ensures proper cleanup by:
- Closing connections properly
- Handling context cancellation
- Removing stale registrations

## Example

Here's a complete example showing how to use the library:

```go
package main

import (
    "context"
    "log"
    "time"
    etcdregistry "go.lumeweb.com/etcd-registry"
)

func main() {
    // Create registry
    registry, err := etcdregistry.NewEtcdRegistry(
        []string{"localhost:2379"},
        "/services",
        "",
        "",
        10*time.Second,
    )
    if err != nil {
        log.Fatal(err)
    }

    // Create a node
    node := etcdregistry.Node{
        Name: "api-server-1",
        Info: map[string]string{
            "version": "1.0.0",
            "address": "localhost:8080",
        },
    }

    // Create context with cancellation
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Register node
    done, errChan, err := registry.RegisterNode(ctx, "api", node, 30*time.Second)
    if err != nil {
        log.Fatal(err)
    }

    // Wait for registration
    select {
    case <-done:
        log.Println("Node registered successfully")
    case err := <-errChan:
        log.Printf("Registration failed: %v", err)
        return
    }

    // Get all nodes
    nodes, err := registry.GetServiceNodes("api")
    if err != nil {
        log.Fatal(err)
    }

    for _, n := range nodes {
        log.Printf("Found node: %s with info: %v\n", n.Name, n.Info)
    }

    // Keep the program running
    <-ctx.Done()
}
```

## Testing

The library includes comprehensive tests. Run them using:

```bash
go test -v
```

## Requirements

- Go 1.23 or higher
- etcd 3.5 or higher

## License

This project is licensed under the terms of the LICENSE file included in the repository.
