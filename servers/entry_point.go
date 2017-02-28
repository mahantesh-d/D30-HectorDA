package servers

import (
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/constant"
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/model"
	"github.com/dminGod/D30-HectorDA/utils"
	"github.com/dminGod/D30-HectorDA/etcd"
	"os"

)

var serverType string

// Conf is used to store external Config information
var Conf config.Config

// RequestAbstract acts as a wrapper for the mapping the incoming request
var RequestAbstract model.RequestAbstract

// Server is used to start a TCP server
func Server(serverTypePassed string) {

	// send heartbeat to etcd
	go etcd.Heartbeat()

	// call a named pipe to listen for graceful shutdown
	// go NamedPipe()

	// Set the server Type
	serverType = serverTypePassed

	// mode
	logger.Write("INFO", "Server Mode : "+serverType)
	if serverType == constant.HectorProtobufMode {

		// TODO: Integration with native protobuf
		// entry point for native protobuf will come here

	} else if serverType == constant.HectorGrpcMode {

		GRPCStartServer()
	} else if serverType == constant.HTTP {

		HTTPStartServer()
	}

}

// NamedPipe runs in the background and listens for a server stop event
func NamedPipe() {

	// remove any pipe if exists
	os.Remove(constant.HectorPipe)

	// create a named pipe
	// err := syscall.Mkfifo(constant.HectorPipe, 0666)

	logger.Write("INFO", "Listening for Graceful shutdown")

	//if err != nil {
	//	logger.Write("INFO", "Error creating named pipe"+err.Error())
	//	utils.Exit(1)
	//}

	file, err := os.OpenFile(constant.HectorPipe, os.O_CREATE, os.ModeNamedPipe)

	if err != nil {
		logger.Write("INFO", "Error listening for named pipe"+err.Error())
		utils.Exit(1)
	}

	logger.Write("INFO", "Graceful shutdown")
	file.Close()
	utils.Exit(0)
}
