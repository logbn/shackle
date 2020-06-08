package config

import (
	"time"
)

type App struct {
	Api  Api  `yaml:"api"`
	Data Data `yaml:"data"`
	Repo Repo `yaml:"repo"`
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

type Repo struct {
	Hash *Hash `yaml:"hash"`
}

type Hash struct {
	IndexPath      string        `yaml:"ixpath"`
	TimeseriesPath string        `yaml:"tspath"`
	Partitions     int           `yaml:"partitions"`
	Replicas       int           `yaml:"replicas"`
	Surrogates     int           `yaml:"surrogates"`
	KeyExpiration  time.Duration `yaml:"key_expiration"`
	LockExpiration time.Duration `yaml:"lock_expiration"`
	CacheSize      int           `yaml:"cachesize"`
}

type Log struct {
	Level string `yaml:"level"`
}
