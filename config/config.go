package config

import (
	"fmt"
	"github.com/spf13/viper"
)

type cassandra struct {
	Host string
	Port string
}

type hector struct {
	ConnectionType string
	Version        string
	Host           string
	Port           string
	Log            string
}

type Config struct {
	Cassandra cassandra
	Hector    hector
	loaded    bool
}

var Conf Config

func Init() {

	viper.SetConfigName("config") // path to look for the config file in
	viper.AddConfigPath("D30-HectorDA/")
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

func Get() Config {

	if Conf.loaded != true {

		Init()
	}

	return Conf
}
