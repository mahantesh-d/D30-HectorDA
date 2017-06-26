package config

import (
	"fmt"
	"github.com/dminGod/D30-HectorDA/constant"
	"github.com/oleiade/reflections"
	"github.com/spf13/viper"
	_ "github.com/spf13/viper/remote"
	"os"
)

// cassandra struct represents the configuration parameters for the Cassandra endpoint
type cassandra struct {
	Host []string
	Username string
	Password string
}

type etcd_configuration struct {

	Etcd_servers []string
	Etcd_connection_url string
	Etcd_key string
	Etcd_heartbeat_key string
	Etcd_heartbeat_ttl int
	Etcd_heartbeat_message_interval int
	Etcd_fetch_config_interval int
}

// hector struct represents the configuration parameters for the hector server
type hector struct {
	ConnectionType string
	Version string
	Host string
	Port string
	Log string
	LogDirectory string
	StartServersOfType []string
	RequestMetrics bool
	QueryMetrics bool
	PortHTTP string
	DefaultRecordsLimit string
	MaxLimitAllowedByAPI string
	AsyncProcessRequests bool
	ManipulateData	bool
}

type postgresxl struct {

	Username string
	Password string
	Database string
	Port string
	Host string
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
	Postgresxl  postgresxl
	loaded    bool
}

// Conf contains all the configuration information
var Conf Config

var Alltrade_get string
var Alltrade_insert string
var metadata_get map[string]interface{}
var metadata_insert map[string]interface{}


//var ConfPathHash = map[string]string {
//	//"alltrade_get" : constant.HectorConf + "alltrade_unified.json",
//	//"alltrade_insert" : constant.HectorConf + "alltrade_unified.json",
//}

func Init() {

	if etcdInit() != nil {
		fmt.Println("Getting localconfig")
		localInit()
	}

	Alltrade_get = readFile(constant.HectorConf + "/alltrade_unified.json")
	Alltrade_insert = readFile(constant.HectorConf + "/alltrade_unified.json")

	metadata_get = decodeJSON(Alltrade_get)
	metadata_insert = decodeJSON(Alltrade_insert)

	fmt.Println("Size of Alltrade_get : ", len(Alltrade_get), "Size of Alltrade_insert : ", len(Alltrade_insert))

	if(len(Alltrade_get) == 0 || len(Alltrade_insert) == 0 ){

		fmt.Println("Could not read JSON files, exiting.")
		os.Exit(1)
	}

	if(metadata_get == nil || metadata_insert == nil) {

		if metadata_get == nil {
			fmt.Println("Something is broken with the JSON API GET File, please fix, can't parse it.")
		}

		if metadata_insert == nil {
			fmt.Println("Something is broken with the JSON Insert File, please fix, can't parse it.")
		}

			os.Exit(1)
	}


}

func Metadata_get() map[string]interface{} {

	return decodeJSON(Alltrade_get)
}

func Metadata_insert() map[string]interface{} {

	return decodeJSON(Alltrade_insert)
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

	gotETCDConfig := true

	useFolder := getUseFolder()

	var etcd_conf_data etcd_configuration
	var err error

	var etcd_conf = viper.New()

	etcd_conf.SetConfigName("etcd_config") // path to look for the config file in
	etcd_conf.AddConfigPath(useFolder)
	etcd_conf.SetConfigType("toml")

	err = etcd_conf.ReadInConfig()

	if err != nil {

		fmt.Println("ERROR", "Got error in reading ETCD Config", err.Error())
		gotETCDConfig = false
		return err
	}

	// Getting the ETCD details
	etcd_conf.Unmarshal( &etcd_conf_data )

	// Override the constants from the etcd file
	constant.EtcdMessageInterval = etcd_conf_data.Etcd_heartbeat_message_interval
	constant.EtcdEndpoints = etcd_conf_data.Etcd_servers
	constant.EtcdTTL = etcd_conf_data.Etcd_heartbeat_ttl
	constant.EtcdHeartbeatDirectory = etcd_conf_data.Etcd_heartbeat_key

	if gotETCDConfig {

		fmt.Println("INFO", "GOT ETCD Configuration, ", etcd_conf_data)
	}

	var runtime_viper = viper.New()
	runtime_viper.AddRemoteProvider( "etcd", etcd_conf_data.Etcd_connection_url, etcd_conf_data.Etcd_key )
	runtime_viper.SetConfigType( constant.EtcdConfigType )

	err = runtime_viper.ReadRemoteConfig()

	if err != nil {

		fmt.Println("Error reading config from ETCD", err.Error())
		return err
	}

	err = runtime_viper.Unmarshal(&Conf)

	if err != nil {

		fmt.Println("Error in parsing the toml file", err.Error())
		return err
	}


	fmt.Println("The parsed config is", Conf)

	go func(etcd_conf_data etcd_configuration) {
		for {

			fmt.Println("Getting configuration from ETCD... start")
			// time.Sleep(time.Second * 3) // delay after each request

			// currently, only tested with etcd support
			err := runtime_viper.WatchRemoteConfig()

			if err != nil {

				fmt.Println("error in trying to fetch remote config", err.Error())
				continue
			}

			fmt.Println("Getting configuration from ETCD...")

			// unmarshal new config into our runtime config struct. you can also use channel
			// to implement a signal to notify the system of the changes

			runtime_viper.Unmarshal(&Conf)
			fmt.Println("My config is now...", Conf)
		}
	}(etcd_conf_data)


	Conf.loaded = true
	return nil
}

func localInit() {


	useFolder := getUseFolder()// "/etc/hector"

	fmt.Println("Using configuration folder " + useFolder)

	viper.SetConfigName("config") // path to look for the config file in
	viper.AddConfigPath(useFolder)
	viper.SetConfigType("toml")
	err := viper.ReadInConfig()

	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	viper.Unmarshal(&Conf)

	Conf.loaded = true
}

func getUseFolder() string {

	configuredFileLocation := constant.HectorConf + "/config.toml"

	useFolder := "/etc/hector"

	if _, err := os.Stat(configuredFileLocation); err == nil {

		// path/to/whatever exists
		useFolder = constant.HectorConf

	} else if _, err := os.Stat("/etc/hector/config.toml"); err == nil {


		useFolder = "/etc/hector"
	} else if _, err := os.Stat("conf-example/config.toml"); err == nil {

		useFolder = "conf-example"
	}

	return useFolder
}


