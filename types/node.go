package types

import "time"

// Node represents a single exporter instance
type Node struct {
	ID           string            `json:"id"`
	ExporterType string            `json:"exporter_type"`
	Port         int               `json:"port"`
	MetricsPath  string            `json:"metrics_path"`
	Labels       map[string]string `json:"labels"`
	Status       string            `json:"status"`
	LastSeen     time.Time         `json:"last_seen"`
}
