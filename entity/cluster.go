package entity

import (
	"encoding/json"
	"time"
)

const (
	CLUSTER_MIGRATION_TYPE_SCALE_UP            = "scale_up"
	CLUSTER_MIGRATION_TYPE_NODE_REPLACEMENT    = "node_replacement"
	CLUSTER_MIGRATION_TYPE_CLUSTER_REPLACEMENT = "cluster_replacement"

	CLUSTER_MIGRATION_STATUS_PENDING    = "pending"
	CLUSTER_MIGRATION_STATUS_ACTIVE     = "active"
	CLUSTER_MIGRATION_STATUS_WAITING    = "waiting"
	CLUSTER_MIGRATION_STATUS_FINALIZING = "finalizing"
	CLUSTER_MIGRATION_STATUS_COMPLETE   = "complete"
	CLUSTER_MIGRATION_STATUS_CANCELLED  = "cancelled"
)

// ClusterManifest
type ClusterManifest struct {
	Catalog    ClusterCatalog     `json:"catalog"`
	Migrations []ClusterMigration `json:"migrations"`
}

func (e *ClusterManifest) ToJson() (data []byte) {
	data, _ = json.Marshal(e)
	return
}

func (e *ClusterManifest) FromJson(data []byte) error {
	return json.Unmarshal(data, e)
}

type ClusterCatalog struct {
	Version    string             `json:"version"`
	Replicas   int                `json:"k"`
	Vary       []string           `json:"vary"`
	Nodes      []ClusterNode      `json:"nodes"`
	VNodes     []ClusterVNode     `json:"vnodes"`
	Partitions []ClusterPartition `json:"partitions"`
}

type ClusterNode struct {
	ID     string `json:"id"`
	IP     string `json:"ip"`
	Leader bool   `json:"leader"`
	Zone   string `json:"zone"`
}

type ClusterVNode struct {
	ID       string `json:"id"`
	Node     int    `json:"node"`
	Capacity int    `json:"cap"`
}

type ClusterPartition struct {
	Prefix     int   `json:"p"`
	Master     int   `json:"m"`
	Replicas   []int `json:"r"`
	Surrogates []int `json:"s"`
}

type ClusterMigration struct {
	Type       string         `json:"type"`
	Status     string         `json:"status"`
	Version    int            `json:"version"`
	Progress   int            `json:"progress"`
	StartAfter time.Time      `json:"start_after"`
	From       ClusterCatalog `json:"from"`
	To         ClusterCatalog `json:"to"`
}
