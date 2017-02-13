package config

import (
	"fmt"
	"github.com/spf13/viper"
	"github.com/oleiade/reflections"
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

type presto struct {
	ConnectionURL string
}

type Config struct {
	Cassandra cassandra
	Presto presto
	Hector    hector
	loaded    bool
}

var Conf Config

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

func Get() Config {

	if Conf.loaded != true {

		Init()
	}

	return Conf
}

func GetHectorConfig(setting string) string {

	if Conf.loaded != true {

		Init()
	}

	retval, _ := reflections.GetField(Conf.Hector, setting)

	return retval.(string)
}
