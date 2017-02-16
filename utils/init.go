package utils

import (
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/logger"
	"os"
)

// Init triggers the initialization of config
func Init() {

	// Initialise the configuration
	config.Init()

	logger.Write("INFO", "Hector initialize called, Version : " + config.Conf.Hector.Version)
}

// AppExit causes the server to exit
func AppExit(message string) {

	logger.Write("ERROR", "Hector Stopped: '" + message + "' ")
	logger.Write("ERROR", "Exiting with error code 1")
	os.Exit(1)
}
