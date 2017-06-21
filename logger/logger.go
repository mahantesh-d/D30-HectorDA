package logger

import (
	"github.com/dminGod/D30-HectorDA/config"
	// "log"
	lg "github.com/antigloss/go/logger"
	"log"
	"fmt"
)

// AllowedLevels specify the logging levels that are permitted in the application
var AllowedLevels = []string{"VERBOSE","DEBUG","INFO","WARN","ERROR"}
var logLevelInt int
func init() {
	Conf := config.Get()
	logLevel :=  Conf.Hector.Log
	logLevelInt = arrayIndex(AllowedLevels, logLevel)
	// f, err := os.OpenFile(Conf.Hector.LogDirectory + "/server.log", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0660)

	//if err != nil {
	//	fmt.Println(err.Error())
	//	os.Exit(1)
	//} else {
	//	log.SetOutput(f)
	//}

	err := lg.Init(Conf.Hector.LogDirectory, // specify the directory to save the logfiles
		100, // maximum logfiles allowed under the specified log directory
		20, // number of logfiles to delete when number of logfiles exceeds the configured limit
		200, // maximum size of a logfile in MB
		false) // whether logs with Trace level are written down

	if err != nil {

		fmt.Println("Error is : ", err.Error())
	}
}

// Write is used to log to the configured log file ( configuration set by Viper )
// For example:
//  Write("INFO", "Writing to the log file")
// Output:
//  2017/02/16 05:40:21  [ INFO ] Writing to the log file
func Write(level string, messageIn ...interface{}) {

	levelInt := arrayIndex(AllowedLevels,level)

	if levelInt < logLevelInt {

		return
	}

	var message string


	if _, ok := messageIn[0].(string); ok && len(messageIn) == 1 {

		message = messageIn[0].(string)
	} else {

		for _, v := range messageIn {

			message += fmt.Sprint(v) + " "
		}
	}

	switch level {

	case "VERBOSE", "DEBUG", "INFO":
		lg.Info(message)

	case "WARN":
		lg.Warn(message)

	case "ERROR":
		lg.Error(message)

	}

	// if levelInt >= logLevelInt && logLevelInt != -1 {
	//	put(level, message)
	// }
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
