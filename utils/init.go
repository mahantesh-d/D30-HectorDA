package utils

import (
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/logger"
)

var Conf config.Config

//var HectorSession model.HectorSession

func Init() {

	// Initialise the configuration
	config.Init()

	Conf = config.Get()

	logger.Write("INFO", "Hector "+Conf.Hector.Version, Conf.Hector.Log)
}
