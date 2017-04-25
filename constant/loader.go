package constant

import (
	"flag"
	"github.com/dminGod/HowRU/utils"
	"fmt"
	"github.com/spf13/viper"
)

var EtcdEndpoints []string


func init_wip(){

	// Load the file from where ever you can find it.
	// Env variable : D30HECTOR_CONST_DIR
	// Flag parameter : --D30HECTOR_CONST_DIR
	// Folders in this order :
	// 		/opt/damocles/conf/d30hector_const.toml
	//		/etc/hector/d30hector_const.toml
	//		C:\go_code\src\github.com\dminGod\D30-HectorDA\conf-example\d30hector_const.toml

	var constDirFlag = flag.String("D30HECTOR_CONST_DIR", "", "You can set the path of the config file dir here eg: /opt/damocles/conf/")
	fileName := "d30hector_const.toml"

	flag.Parse()

	constFileLocation := ""
	passedFlagFileExists := false
	commonConstantsSet := false

	// Something is passed in the param, see if its valid or not...
	if constDirFlag != "" {  passedFlagFileExists, _ = utils.Exists( constDirFlag + fileName ) }

	// If flag is not there or the file does not exist....
	if constDirFlag == "" || !passedFlagFileExists {

		// Possible directories
		possibleConstDirectories := []string{"/opt/damocles/conf/", "/etc/hector/", "C:\\go_code\\src\\github.com\\dminGod\\D30-HectorDA\\conf-example\\"}


		for dirs := range possibleConstDirectories {

			chk, err := utils.Exists( dirs + fileName )

			if chk && err == nil {

				fmt.Println("Setting the cosntants path as : ", dirs + fileName)
				constFileLocation = dirs + fileName
				break
			}
		}

		// After looking everywhere we still didn't find anything then we set the default values
		if constFileLocation == "" {

			fmt.Println("Count not find the constant dir from common folders, setting common values...")
			set_common_constants()
			commonConstantsSet = true
		}

	} else {

		fmt.Println("Flag was passed from the command line and the passed file was found, using this", constDirFlag + fileName)

		constFileLocation = constDirFlag + fileName
	}


	// We will now try to pull this file and populate the constants
	if constFileLocation != "" && commonConstantsSet == false {

		viper.SetConfigFile(constFileLocation) // path to look for the config file in
		viper.SetConfigType("toml")
		err := viper.ReadInConfig()

		if err != nil {

			fmt.Println("Some issue with getting values from the file", constFileLocation, " Using config values. Error is: '", err, "'")
		}
	}


}


// If none of the files are found then the common values will be set

func set_common_constants() {


// ETCD Constants :
	// EtcdConnectionURL is the Connection URL to fetch configuration from etcd
	const EtcdConnectionURL string = "http://10.138.32.217:2379"

	// EtcdEndpoints is the list of contact points in the etcd cluster
	EtcdEndpoints = []string{ "http://10.138.32.217:2379","http://10.138.32.218:2379", "http://10.138.32.219:2379",
		"http://10.138.32.220:2379","http://10.138.32.221:2379","http://10.138.32.222:2379",  }

	// EtcdKey is the Key within etcd server which contains the configuration information
	const EtcdKey string = "/hector/config/config.toml"

	// EtcdConfigType is the extension of the values in etcd
	const EtcdConfigType string = "toml"

	// EtcdHeartbeatDirectory is the Key within etcd server which contains the list of active hector instances
	const EtcdHeartbeatDirectory string = "/hector/active-servers"

	// TTL of heartbeat message in seconds
	const EtcdTTL int = 5

	// EtcdMessageInterval is the interval of heartbeat
	const EtcdMessageInterval int = 3


// Hector Constants :

	// HectorPipe is the Named pipe used to listen for graceful server shutdown
	const HectorPipe = "/tmp/hector"

	// HectorConf is the path of the configuration file
	const HectorConf = "/etc/hector"
	// const HectorConf = "conf-example"

	// HectorGrpcMode is the GRPC server mode
	const HectorGrpcMode string = "grpc"

	// HectorProtobufMode is the native protobuf server mode
	const HectorProtobufMode string = "protobuf"

	// HTTP is the HTTP server mode
	const HTTP string = "http"

	// HectorRouteDelimiter is the delimiter used for route mapping
	const HectorRouteDelimiter = "_"
}




