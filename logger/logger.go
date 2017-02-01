package logger

import (
	"log"
	"github.com/dminGod/D30-HectorDA/config"
)

var AllowedLevels []string = []string{"INFO", "ERROR", "DEBUG"};

func Write(level string, msg string) {

	configLevel := config.GetHectorConfig("Log")

	message := " [ " + level + " ] " + msg

	// If this guy tried to log something other than allowed levels
	if ! containsStr(AllowedLevels, level) {

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


func containsStr(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}

	return false
}
