package main

import (
	"flag"

	"github.com/Avash027/midDB/LsmTree"
	"github.com/Avash027/midDB/config"
	"github.com/Avash027/midDB/logger"
	"github.com/Avash027/midDB/server"
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
		LsmTree: LsmTree.InitNewLSMTree(
			serverConfig.DBEngineConfig.MaxElementsBeforeFlush,
			serverConfig.DBEngineConfig.CompactionFrequency,
		),
	}

	server.Start()

}
