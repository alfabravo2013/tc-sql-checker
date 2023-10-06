package main

import (
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

type MyConfig struct {
	PoolSize uint8 `yaml:"pool-size"`
}

func GetMyConfig() *MyConfig {
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Printf("Failed to load config.yaml, applying defaults")
		return &MyConfig{
			PoolSize: 4,
		}
	}

	var config MyConfig
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		log.Printf("Failed to load config.yaml, applying defaults")
		return &MyConfig{
			PoolSize: 4,
		}
	}

	return &config
}
