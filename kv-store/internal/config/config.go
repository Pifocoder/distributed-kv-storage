package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type ClusterConfig struct {
	Host              string `yaml:"host"`
	Port              string `yaml:"port"`
	HealthIntervalSec int    `yaml:"health_interval_sec"`
	SeedAddr          string `yaml:"seed_addr"`
}

type HashConfig struct {
	VNodesPerNode int `yaml:"vnodes_per_node"`
}

type Config struct {
	Cluster ClusterConfig `yaml:"cluster"`
	Hash    HashConfig    `yaml:"hash"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
