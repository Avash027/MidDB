package config

import (
	"io/ioutil"
	"path/filepath"

	"gopkg.in/yaml.v2"
)

type Config struct {
	Server          ServerConfig    `yaml:"server,inline"`
	DBEngineConfig  DBEngineConfig  `yaml:"db_engine,inline"`
	DiskStoreConfig DiskStoreConfig `yaml:"disk_store,inline"`
}

type ServerConfig struct {
	Port          string `yaml:"port"`
	Host          string `yaml:"host"`
	UDPPort       string `yaml:"udp_port"`
	UDPBufferSize int    `yaml:"udp_buffer_size"`
}

type DiskStoreConfig struct {
	NumOfPartitions int    `yaml:"num_of_partitions"`
	Directory       string `yaml:"directory"`
}
type DBEngineConfig struct {
	LSMTreeConfig     LSMTreeConfig     `yaml:"lsm_tree,inline"`
	BloomFilterConfig BloomFilterConfig `yaml:"bloom_filter,inline"`

	WalPath string `yaml:"wal_path"`
}

type LSMTreeConfig struct {
	MaxElementsBeforeFlush int `yaml:"max_elements_before_flush"`
	CompactionFrequency    int `yaml:"compaction_frequency_in_ms"`
}

type BloomFilterConfig struct {
	Capacity  int     `yaml:"bloom_capacity"`
	ErrorRate float64 `yaml:"bloom_error_rate"`
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
