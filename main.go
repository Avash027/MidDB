package main

import (
	"flag"

	"github.com/Avash027/midDB/config"
	dbengine "github.com/Avash027/midDB/db_engine"
	"github.com/Avash027/midDB/logger"
	LsmTree "github.com/Avash027/midDB/lsm_tree"
	"github.com/Avash027/midDB/server"
	"github.com/Avash027/midDB/wal"
)

func main() {
	var configFile string
	flag.StringVar(&configFile, "config", "config.yaml", "Path to config file")
	flag.Parse()

	serverConfig, err := config.ParseConfig(configFile)

	if err != nil {
		panic(err)
	}

	logger.LoggerInit(serverConfig.LoggerConfig.Mode)

	server := server.Server{
		Port: serverConfig.Server.Port,
		Host: serverConfig.Server.Host,
		DBEngine: &dbengine.DBEngine{
			LsmTree: LsmTree.InitNewLSMTree(
				serverConfig.DBEngineConfig.MaxElementsBeforeFlush,
				serverConfig.DBEngineConfig.CompactionFrequency),
			Wal: wal.InitWAL(serverConfig.DBEngineConfig.WalPath),
		},
	}

	server.Start()

}
