package types

import (
	"time"
)

// ServiceGroupSpec defines the configuration for a service group
type ServiceGroupSpec struct {
	Username     string            `json:"username"`     // Auth username
	Password     string            `json:"password"`     // Auth password
	CommonLabels map[string]string `json:"common_labels,omitempty"` // Labels applied to all nodes
}

// ServiceGroupData represents the stored group data
type ServiceGroupData struct {
	Name    string          `json:"name"`    // Unique group identifier
	Spec    ServiceGroupSpec `json:"spec"`    // Group configuration
	Created time.Time       `json:"created"` // When group was created
}
