package types

// WatchEventType represents the type of watch event
type WatchEventType string

const (
	EventTypeCreate WatchEventType = "create"
	EventTypeUpdate WatchEventType = "update"
	EventTypeDelete WatchEventType = "delete"
)

// WatchEvent represents a change event from etcd
type WatchEvent struct {
	Type      WatchEventType `json:"type"`
	GroupName string         `json:"group_name"`
	Node      *Node          `json:"node,omitempty"`
}
