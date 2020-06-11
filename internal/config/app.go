package config

import (
	"time"
)

type App struct {
	Api  Api  `yaml:"api"`
	Data Data `yaml:"data"`
	Repo Repo `yaml:"repo"`
	Raft Raft `yaml:"raft"`
	Log  Log  `yaml:"log"`
}

type Api struct {
	Enabled bool    `yaml:"enabled"`
	Http    ApiHttp `yaml:"http"`
}

type ApiHttp struct {
	Enabled         bool          `yaml:"enabled"`
	IdleTimeout     time.Duration `yaml:"idle_timeout"`
	Keepalive       bool          `yaml:"keepalive"`
	KeepalivePeriod time.Duration `yaml:"keepalive_period"`
	MaxConnsPerIP   int           `yaml:"max_conns_per_ip"`
	Port            string        `yaml:"port"`
	ReadTimeout     time.Duration `yaml:"read_timeout"`
	WriteTimeout    time.Duration `yaml:"write_timeout"`
}

type Data struct {
	Port int `yaml:"port"`
}

type Raft struct {
	ClusterID string `yaml:"clusterid"`
	NodeID    string `yaml:"nodeid"`
	Port      int    `yaml:"port"`
	Join      string `yaml:"join"`
}

type Repo struct {
	Hash *RepoHash `yaml:"hash"`
}

type RepoHash struct {
	CacheSize      int           `yaml:"cachesize"`
	IndexPath      string        `yaml:"ixpath"`
	KeyExpiration  time.Duration `yaml:"key_expiration"`
	KeyLength      int           `yaml:"length"`
	LockExpiration time.Duration `yaml:"lock_expiration"`
	Partitions     int           `yaml:"partitions"`
	SweepInterval  time.Duration `yaml:"sweep_interval"`
	TimeseriesPath string        `yaml:"tspath"`
}

type Log struct {
	Level string `yaml:"level"`
}

type Cluster struct {
	Replicas   int `yaml:"replicas"`
	Surrogates int `yaml:"surrogates"`
}
