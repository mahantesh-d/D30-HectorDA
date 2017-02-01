package logger

import (
	"log"
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/utils"
)

var AllowedLevels []string = []string{"INFO", "ERROR", "DEBUG"};

func Write(level string, msg string) {

	configLevel := config.GetHectorConfig("Log")

	message := " [ " + level + " ] " + msg

	// If this guy tried to log something other than allowed levels
	if ! utils.ContainsStr(AllowedLevels, level) {

		log.Printf("[Error] Irony! Another level or error, log type is wrong, changing original type '" + level + "' to ERROR")
		level = "ERROR"
	}

	if configLevel == "INFO" {

		log.Printf(message)
	} else if configLevel == "DEBUG" {

		if level == "DEBUG" || level == "ERROR" {

			log.Printf(message)
		}
	} else if configLevel == "ERROR" {

		if level == "ERROR" {
			log.Printf(message)
		}
	}

}
