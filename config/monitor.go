package config

type Monitor struct {
	Prometheus *Prometheus `yaml:"prometheus"`
	Log        *Log        `yaml:"log"`
}

type Prometheus struct {
	Port string `yaml:"port"`
}
