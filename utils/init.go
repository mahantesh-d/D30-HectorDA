package utils

import (
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/logger"
	"os"
)

var Conf config.Config

//var HectorSession model.HectorSession

func Init() {

	// Initialise the configuration
	config.Init()

	logger.Write("INFO", "Hector initialize called, Version : " + Conf.Hector.Version)
}

func AppExit(message string) {

	logger.Write("ERROR", "AppExit called with message --> '" + message + "' ")
	logger.Write("ERROR", "Exiting with error code 1")
	os.Exit(1)
}

func ContainsStr(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}

	return false
}