package logger

import (
	"fmt"
	"github.com/dminGod/D30-HectorDA/config"
	"log"
	"os"
)

// AllowedLevels specify the logging levels that are permitted in the application
var AllowedLevels = []string{"INFO", "ERROR", "DEBUG"}

func init() {
	Conf := config.Get()
	f, err := os.OpenFile(Conf.Hector.LogDirectory+"/server.log", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0660)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	} else {
		log.SetOutput(f)
	}
}

// Write is used to log to the configured log file ( configuration set by Viper )
// For example:
//  Write("INFO", "Writing to the log file")
// Output:
//  2017/02/16 05:40:21  [ INFO ] Writing to the log file
func Write(level string, msg string) {

	configLevel := config.GetHectorConfig("Log")

	message := " [ " + level + " ] " + msg

	// If this guy tried to log something other than allowed levels
	if !containsStr(AllowedLevels, level) {

		log.Printf("[Error] Irony! Another level or error, log type is wrong, changing original type '" + level + "' to ERROR")
		level = "ERROR"
	}

	if configLevel == "DEBUG" {

		log.Printf(message)
	} else if configLevel == "INFO" {

		if level == "INFO" {

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

func Metric(msg string) {
	config := config.Get()

	// check if logging is enabled
	if config.Hector.RequestMetrics {
		log.Printf("[Metrics] " + msg)
	}

}
