# ETCD Registry

A Go library for service registration and discovery using etcd. This library provides a hierarchical system of service groups and nodes, with support for common configuration and label inheritance.

## Features

- Service groups with shared configuration
- Node registration with TTL (Time To Live)
- Common labels inheritance from group to nodes
- Automatic registration renewal
- Service discovery at group and node level
- Authentication support per group
- Configurable timeouts
- Error handling and automatic retries
- Graceful cleanup on context cancellation
- Automatic cleanup of empty groups

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
    3,                          // max retries
)
if err != nil {
    log.Fatal(err)
}
```

### Creating or Joining a Service Group

```go
// Create/join a service group
group, err := registry.CreateOrJoinServiceGroup(ctx, "my-service")
if err != nil {
    log.Fatal(err)
}

// Configure the group
spec := types.ServiceGroupSpec{
    Username: "group-user",        // Auth credentials for this group
    Password: "group-pass",
    CommonLabels: map[string]string{
        "environment": "production",
        "region": "us-east",
    },
}

err = group.Configure(spec)
if err != nil {
    log.Fatal(err)
}
```

### Registering a Node

```go
node := types.Node{
    ID:           "node1",
    ExporterType: "node_exporter",
    Port:         9100,
    MetricsPath:  "/metrics",
    Labels: map[string]string{
        "instance": "i-1234567",  // Node-specific labels
    },
}

// Register with 30-second TTL
done, errChan, err := group.RegisterNode(ctx, node, 30*time.Second)
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

### Service Discovery

```go
// List all service groups
groups, err := registry.GetServiceGroups(ctx)
if err != nil {
    log.Fatal(err)
}

// Get group configuration
group, err := registry.GetServiceGroup(ctx, "my-service")
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Group: %s, Common Labels: %v\n", group.Name, group.Spec.CommonLabels)

// Get nodes in a group
nodes, err := group.GetNodes(ctx)
if err != nil {
    log.Fatal(err)
}

for _, node := range nodes {
    // Node labels include both group common labels and node-specific labels
    fmt.Printf("Node: %s, Type: %s, Labels: %v\n", 
               node.ID, node.ExporterType, node.Labels)
}
```

## Configuration

### Registry Configuration

The registry can be configured with the following parameters:

- `etcdEndpoints`: List of etcd server endpoints (required)
- `etcdBasePath`: Base path for all etcd operations (required)
- `etcdUsername`: Username for etcd authentication (optional)
- `etcdPassword`: Password for etcd authentication (optional)
- `defaultTimeout`: Default timeout for etcd operations (required)
- `maxRetries`: Maximum number of retry attempts for operations (required)

### Service Group Configuration

Each service group can have its own configuration:

- `Username`: Authentication username for the group
- `Password`: Authentication password for the group
- `CommonLabels`: Labels that are automatically applied to all nodes in the group

### Node Configuration

Nodes require the following configuration:

- `ID`: Unique identifier for the node
- `ExporterType`: Type of metrics exporter (e.g., "node_exporter")
- `Port`: Port number where metrics are exposed
- `MetricsPath`: HTTP path where metrics are available
- `Labels`: Node-specific labels (merged with group's common labels)

## Features in Detail

### Service Groups

Service groups provide:
- Logical grouping of related nodes
- Shared authentication credentials
- Common labels inherited by all nodes
- Automatic cleanup when empty
- Name validation and sanitization

### Automatic Registration Renewal

The library automatically maintains node registrations by:
- Refreshing the TTL periodically (at half the TTL interval)
- Handling connection failures with exponential backoff
- Retrying failed registrations automatically
- Inheriting and merging group labels

### Error Handling

The library provides comprehensive error handling:
- Connection failures with automatic reconnection
- Authentication errors at both registry and group level
- Timeout handling with configurable retries
- Context cancellation
- Label validation
- Group name validation

### Cleanup

The library ensures proper cleanup by:
- Closing connections properly
- Handling context cancellation
- Removing stale registrations
- Automatically cleaning up empty groups
- Proper lease management

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
