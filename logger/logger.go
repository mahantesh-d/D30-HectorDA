package logger

import (
	"fmt"
	"github.com/dminGod/D30-HectorDA/config"
	"log"
	"os"
)

// AllowedLevels specify the logging levels that are permitted in the application
var AllowedLevels = []string{"VERBOSE","DEBUG","INFO","WARN","ERROR"}
var logLevelInt int
func init() {
	Conf := config.Get()
	logLevel :=  Conf.Hector.Log
	logLevelInt = arrayIndex(AllowedLevels, logLevel)
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
func Write(level string, message string) {
	levelInt := arrayIndex(AllowedLevels,level)
	if levelInt >= logLevelInt && logLevelInt != -1 {
		put(level, message)
	}
}


func put(level string, msg interface{}) {
	switch dataType := msg.(type) {
		case string:
			message := " [ " + level + " ] " + msg.(string)
			log.Printf(message)
			_ = dataType
	}
}

func Metric(msg string) {
	config := config.Get()

	// check if logging is enabled
	if config.Hector.RequestMetrics {
		log.Printf("[Metrics] " + msg)
	}

}


func arrayIndex(array interface{}, element interface{}) int {
        var index int
        switch dataType := element.(type) {
                case string:
                        index = stringArrayIndex(array.([]string), element.(string))
                        _ = dataType
        }
        return index
}

func stringArrayIndex(array []string, element string) int {
        for k,v := range array {
                if v == element {
                        return k
                }
        }
        return -1
}
