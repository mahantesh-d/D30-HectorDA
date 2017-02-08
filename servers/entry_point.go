package servers

import (
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/model"
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/constant"
)

var serverType string

var Conf config.Config
var RequestAbstract model.RequestAbstract

// Used to start a TCP server
func Server(serverTypePassed string) {

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
