package config

import (
	"time"
)

type App struct {
	Api     Api     `yaml:"api"`
	Cluster Cluster `yaml:"cluster"`
	Data    Data    `yaml:"data"`
	Log     Log     `yaml:"log"`
	Monitor Monitor `yaml:"monitor"`
	Repo    Repo    `yaml:"repo"`
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
	Port            int           `yaml:"port"`
	ReadTimeout     time.Duration `yaml:"read_timeout"`
	WriteTimeout    time.Duration `yaml:"write_timeout"`
}

type Cluster struct {
	Enabled    bool   `yaml:"enabled"`
	ID         string `yaml:"id"`
	Partitions int    `yaml:"partitions"`
	Raft       Raft   `yaml:"raft"`
	Replicas   int    `yaml:"replicas"`
	Surrogates int    `yaml:"surrogates"`
}

type Data struct {
	Port      int    `yaml:"port"`
	KeyLength int    `yaml:"keylength"`
	Pepper    string `yaml:"pepper"`
}

type Log struct {
	Level string `yaml:"level"`
}

type Raft struct {
	NodeID string `yaml:"node_id"`
	Port   int    `yaml:"port"`
	Join   string `yaml:"join"`
}

type Repo struct {
	Hash *RepoHash `yaml:"hash"`
}

type RepoHash struct {
	CacheSize      int           `yaml:"cachesize"`
	ExpBatchSize   int           `yaml:"expiration_batch_size"`
	KeyExpiration  time.Duration `yaml:"key_expiration"`
	LockExpiration time.Duration `yaml:"lock_expiration"`
	Partitions     int           `yaml:"partitions"`
	PathIndex      string        `yaml:"path_ix"`
	PathTimeseries string        `yaml:"path_ts"`
	SweepInterval  time.Duration `yaml:"sweep_interval"`
}
