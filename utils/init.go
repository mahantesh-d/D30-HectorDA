package utils

import (
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/model"
	"github.com/BurntSushi/toml"
	"os"
)

var Conf model.Config
var HectorSession model.HectorSession

func init() {

	// Parse Config	
	 _,err := toml.DecodeFile("/etc/hector/config.toml",&Conf)
 	if err != nil {
        	logger.Write("ERROR", err.Error(), "ERROR")
         	os.Exit(1)
 	}

	logger.Write("INFO", "Hector " + Conf.Hector.Version, Conf.Hector.Log)
}
