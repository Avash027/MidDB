package config

import (
	"io/ioutil"
	"path/filepath"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Server         ServerConfig   `yaml:"server,inline"`
	LoggerConfig   LoggerConfig   `yaml:"logger,inline"`
	DBEngineConfig DBEngineConfig `yaml:"db_engine,inline"`
}

type ServerConfig struct {
	Port string `yaml:"port"`
	Host string `yaml:"host"`
}

type LoggerConfig struct {
	Mode string `yaml:"mode"`
}

type DBEngineConfig struct {
	MaxElementsBeforeFlush int    `yaml:"max_elements_before_flush"`
	CompactionFrequency    int    `yaml:"compaction_frequency_in_ms"`
	WalPath                string `yaml:"wal_path"`
}

func ParseConfig(filename string) (Config, error) {
	var serverConfig Config
	fname, err := filepath.Abs(filename)

	if err != nil {
		return serverConfig, err
	}

	data, err := ioutil.ReadFile(fname)
	if err != nil {
		return serverConfig, err
	}

	err = yaml.Unmarshal(data, &serverConfig)

	if err != nil {
		return serverConfig, err
	}

	return serverConfig, nil

}
