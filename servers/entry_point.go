package servers

import (
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/model"
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/constant"
	"github.com/dminGod/D30-HectorDA/utils"
	"os"
	"syscall"
)

var serverType string

var Conf config.Config
var RequestAbstract model.RequestAbstract

// Used to start a TCP server
func Server(serverTypePassed string) {

	// call a named pipe to listen for graceful shutdown
	go NamedPipe()

	// Set the server Type
	serverType = serverTypePassed

	// mode
	logger.Write("INFO","Server Mode : " + serverType)
	if serverType == constant.HECTOR_PROTOBUF_MODE {

		// TODO: Integration with native protobuf
		// entry point for native protobuf will come here
	
	} else if serverType == constant.HECTOR_GRPC_MODE {
		GRPCStartServer()
	}
}

func NamedPipe() {

	// remove any pipe if exists
	os.Remove(constant.HECTOR_PIPE)

	// create a named pipe	
	err := syscall.Mkfifo(constant.HECTOR_PIPE, 0666)

	logger.Write("INFO", "Listening for Graceful shutdown")
	
	if err != nil {
		logger.Write("INFO","Error creating named pipe" + err.Error())
		utils.Exit(1)
	}

	file, err := os.OpenFile(constant.HECTOR_PIPE, os.O_CREATE, os.ModeNamedPipe)

	if err != nil {
		logger.Write("INFO","Error listening for named pipe" + err.Error())
		utils.Exit(1)
	}

	logger.Write("INFO", "Graceful shutdown")
	file.Close()
	utils.Exit(0)
}
