package config

import (
	"fmt"
	"github.com/oleiade/reflections"
	"github.com/spf13/viper"
)

// cassandra struct represents the configuration parameters for the Cassandra endpoint
type cassandra struct {
	Host string
	Port string
}

// hector struct represents the configuration parameters for the hector server
type hector struct {
	ConnectionType string
	Version        string
	Host           string
	Port           string
	Log            string
	LogDirectory   string
}

// presto struct represents the configuration parameters for the Presto endpoint
type presto struct {
	ConnectionURL string
}

// Config struct represents the overall configuration comprising of nested cassandra, presto and hector information
type Config struct {
	Cassandra cassandra
	Presto    presto
	Hector    hector
	loaded    bool
}

// Conf contains all the configuration information
var Conf Config

// Init initializes the configuration using viper
func Init() {

	viper.SetConfigName("config") // path to look for the config file in
	viper.AddConfigPath("/etc/hector")
	viper.SetConfigType("toml")

	err := viper.ReadInConfig()

	if err != nil {
		fmt.Println("Config not found...")

		viper.AddConfigPath("/ect/hector/")
		viper.ReadInConfig()
	}

	viper.Unmarshal(&Conf)
	Conf.loaded = true
}

// Get returns the populated configuration information
func Get() Config {

	if Conf.loaded != true {

		Init()
	}

	return Conf
}

// GetHectorConfig returns a specific Hector server setting
// For example:
//  GetHectorConfig("Host")
// Output:
//  127.0.0.1
func GetHectorConfig(setting string) string {

	if Conf.loaded != true {

		Init()
	}

	retval, _ := reflections.GetField(Conf.Hector, setting)

	return retval.(string)
}
