package logger

import (
	"log"
	"github.com/dminGod/D30-HectorDA/config"

)

func Write(level string, msg string) {

	configLevel := config.GetHectorConfig("Log")

	message := " [ " + level + " ] " + msg

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
