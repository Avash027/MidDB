package main

import (
	"flag"
	"fmt"

	"github.com/Avash027/midDB/config"
	dbengine "github.com/Avash027/midDB/db_engine"
	diskstore "github.com/Avash027/midDB/disk_store"

	LsmTree "github.com/Avash027/midDB/lsm_tree"
	"github.com/Avash027/midDB/server"
	"github.com/Avash027/midDB/wal"
)

func main() {
	var configFile string
	flag.StringVar(&configFile, "config", "config.yaml", "Path to config file")
	flag.Parse()

	serverConfig, err := initServerConfig(configFile)

	if err != nil {
		panic(err)
	}

	fmt.Println(serverConfig.DBEngineConfig.LSMTreeConfig.CompactionFrequency)

	lsmTreeOpts := LsmTree.LSMTreeOpts{
		MaxElementsBeforeFlush: serverConfig.DBEngineConfig.LSMTreeConfig.MaxElementsBeforeFlush,
		CompactionPeriod:       serverConfig.DBEngineConfig.LSMTreeConfig.CompactionFrequency,
		BloomFilterOpts: LsmTree.BloomFilterOpts{
			ErrorRate: serverConfig.DBEngineConfig.BloomFilterConfig.ErrorRate,
			Capacity:  serverConfig.DBEngineConfig.BloomFilterConfig.Capacity,
		},
	}
	lsmTree := LsmTree.InitNewLSMTree(lsmTreeOpts)

	diskStoreOpts := diskstore.DiskStoreOpts{
		NumOfPartitions: serverConfig.DiskStoreConfig.NumOfPartitions,
		Directory:       serverConfig.DiskStoreConfig.Directory,
	}
	store := diskstore.New(diskStoreOpts)

	server := server.Server{
		Port:          serverConfig.Server.Port,
		Host:          serverConfig.Server.Host,
		UDPPort:       serverConfig.Server.UDPPort,
		UDPBufferSize: serverConfig.Server.UDPBufferSize,
		DBEngine: &dbengine.DBEngine{
			LsmTree: lsmTree,
			Wal:     wal.InitWAL(serverConfig.DBEngineConfig.WalPath),
			Store:   store,
		},
	}

	server.Start()
}

func initServerConfig(configFile string) (config.Config, error) {
	serverConfig, err := config.ParseConfig(configFile)
	if err != nil {
		return serverConfig, err
	}

	if serverConfig.Server.Port == "" {
		serverConfig.Server.Port = server.DEFAULT_TCP_PORT
	}

	if serverConfig.Server.Host == "" {
		serverConfig.Server.Host = server.DEFAULT_HOST
	}

	if serverConfig.Server.UDPPort == "" {
		serverConfig.Server.UDPPort = server.DEFAULT_UDP_PORT
	}

	if serverConfig.Server.UDPBufferSize == 0 {
		serverConfig.Server.UDPBufferSize = server.DEFAULT_UDP_BUFFER_SIZE
	}

	if serverConfig.DBEngineConfig.WalPath == "" {
		serverConfig.DBEngineConfig.WalPath = wal.DEFAULT_WAL_PATH
	}

	if serverConfig.DBEngineConfig.LSMTreeConfig.MaxElementsBeforeFlush == 0 {
		serverConfig.DBEngineConfig.LSMTreeConfig.MaxElementsBeforeFlush = LsmTree.DEFAULT_MAX_ELEMENTS_BEFORE_FLUSH
	}

	if serverConfig.DBEngineConfig.LSMTreeConfig.CompactionFrequency == 0 {
		serverConfig.DBEngineConfig.LSMTreeConfig.CompactionFrequency = LsmTree.DEFAULT_COMPACTION_FREQUENCY
	}

	if serverConfig.DBEngineConfig.BloomFilterConfig.ErrorRate == 0 {
		serverConfig.DBEngineConfig.BloomFilterConfig.ErrorRate = LsmTree.DEFAULT_BLOOM_FILTER_ERROR_RATE
	}

	if serverConfig.DBEngineConfig.BloomFilterConfig.Capacity == 0 {
		serverConfig.DBEngineConfig.BloomFilterConfig.Capacity = LsmTree.DEFAULT_BLOOM_FILTER_CAPACITY
	}

	if serverConfig.DiskStoreConfig.NumOfPartitions == 0 {
		serverConfig.DiskStoreConfig.NumOfPartitions = diskstore.DEFAULT_NUM_OF_PARTITIONS
	}

	if serverConfig.DiskStoreConfig.Directory == "" {
		serverConfig.DiskStoreConfig.Directory = diskstore.DEFAULT_DIRECTORY
	}

	return serverConfig, nil
}
