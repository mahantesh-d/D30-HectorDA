package servers

import (
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/model"
	"github.com/dminGod/D30-HectorDA/logger"
)

var serverType string

var Conf config.Config
var HectorSession model.HectorSession

// Used to start a TCP server
func Server(serverTypePassed string) {

	// Set the server Type
	serverType = serverTypePassed

	// mode
	logger.Write("INFO","Server Mode : " + serverType)
	if serverType == "protobuf" {
		ProtoStartServer()	
	} else if serverType == "grpc" {
		GRPCStartServer()
	}
}
