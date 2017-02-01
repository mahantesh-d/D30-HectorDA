package logger

import (
	"log"
)

func Write(level string, msg string, configLevel string) {

	message := " [ " + level + " ] " + msg

	println(message)

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
