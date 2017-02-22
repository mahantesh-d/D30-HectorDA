package config

import (
	"fmt"
	"github.com/dminGod/D30-HectorDA/constant"
	"os"
	"github.com/oleiade/reflections"
	"github.com/spf13/viper"
	_ "github.com/spf13/viper/remote"
	"time"
)

// cassandra struct represents the configuration parameters for the Cassandra endpoint
type cassandra struct {
	Host []string
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
/*func Init() {

	viper.SetConfigName("config1") // path to look for the config file in
	viper.AddConfigPath(constant.HectorConf)
	viper.SetConfigType("toml")

	err := viper.ReadInConfig()

	if err != nil {
		fmt.Println("Config not found...")

		viper.AddConfigPath("/etc/hector/")
		viper.ReadInConfig()
	}

	viper.Unmarshal(&Conf)
	Conf.loaded = true
}*/

func Init() {
	
	if etcdInit() != nil {
		fmt.Println("Getting localconfig")
		localInit()
	}
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


func etcdInit() error {
 	var runtime_viper = viper.New()
 	runtime_viper.AddRemoteProvider("etcd", constant.EtcdConnectionURL, constant.EtcdKey)
 	runtime_viper.SetConfigType(constant.EtcdConfigType)
	err := runtime_viper.ReadRemoteConfig()
 	if err != nil {
		return err
	}
 	runtime_viper.Unmarshal(&Conf)
 	
	go func(){
    		for {
        		time.Sleep(time.Second * 5) // delay after each request

        		// currently, only tested with etcd support
        		err := runtime_viper.WatchRemoteConfig()
        		if err != nil {
            			continue
        		}
        		// unmarshal new config into our runtime config struct. you can also use channel
        		// to implement a signal to notify the system of the changes
        		runtime_viper.Unmarshal(&Conf)
		}
	}()
	
	Conf.loaded = true
	return nil
}

func localInit() {
	viper.SetConfigName("config") // path to look for the config file in
	viper.AddConfigPath(constant.HectorConf)
	viper.SetConfigType("toml")
	err := viper.ReadInConfig()

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	viper.Unmarshal(&Conf)
	Conf.loaded = true	
}
