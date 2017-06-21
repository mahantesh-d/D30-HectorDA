package constant

import (
	"os"
	"fmt"
	"strings"
)

// HectorPipe is the Named pipe used to listen for graceful server shutdown
const HectorPipe = "/tmp/hector"

// TODO: 1) Change this to a better folder name -- 2) All the configuration should be outside the binary and in one place

// HectorConf is the path of the configuration file without trailing slash
var HectorConf string = "/opt/damocles/conf"

func init(){

	if _, err := os.Stat("/opt/damocles/conf"); os.IsNotExist(err) {

		HectorConf = "conf-example"

		//if _, err := os.Stat("../conf-example"); os.IsNotExist(err) {
		//
		//	HectorConf = "conf-example"
		//	fmt.Println("Standard director, /etc/hector does not exist, using conf-example")
		//} else {
		//
		//	HectorConf = "../conf-example"
		//	fmt.Println("Standard director, /etc/hector does not exist, using conf-example")
		//}

	}

	serversURLs, valSet := os.LookupEnv("HECTOR_ETCD_SERVERS")

	// Make it blank
	EtcdEndpoints = []string{}

	if valSet && len(serversURLs) > 0 {

		fmt.Println("Taking the config of etcd from environment variable HECTOR_ETCD_SERVERS, it is set to : ", serversURLs)

		serversSplit := strings.Split(serversURLs, ",")

		for _, v := range serversSplit {

			EtcdEndpoints = append(EtcdEndpoints, v)
		}

		EtcdConnectionURL = serversSplit[0]
	}


}


// HectorGrpcMode is the GRPC server mode
const HectorGrpcMode string = "grpc"

// HectorProtobufMode is the native protobuf server mode
const HectorProtobufMode string = "protobuf"

// HTTP is the HTTP server mode
const HTTP string = "http"

// HectorRouteDelimiter is the delimiter used for route mapping
const HectorRouteDelimiter = "_"
