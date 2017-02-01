package servers

import (
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/model"
	"net"
	"github.com/dminGod/D30-HectorDA/utils"
)

var serverType string

var Conf config.Config
var HectorSession model.HectorSession

// Used to start a TCP server
func Server(serverTypePassed string) {

	// Set the server Type
	serverType = serverTypePassed

	Conf = config.Get();

	// listen to the TCP port
	logger.Write("INFO", "Server Starting - host:port - " + Conf.Hector.Host + " : " + Conf.Hector.Port)
 	listener, err := net.Listen(Conf.Hector.ConnectionType, Conf.Hector.Host + ":" + Conf.Hector.Port)

	if err != nil {

		logger.Write("ERROR", "Server Starting Fail - host:port - " + Conf.Hector.Host + " : " + Conf.Hector.Port )
		utils.AppExit("Exiting app, configured port not available")

	} else {

		logger.Write("INFO", "Server Running - host:port - " + Conf.Hector.Host + " : " + Conf.Hector.Port )
	}



	for {
		if conn, err := listener.Accept(); err == nil {

			// if err is nil then that means that data is available for us so we move ahead
			go handleConnection(&conn)
		} else {
			continue
		}
	}
}

func handleConnection(conn *net.Conn) {

	if serverType == "protobuf" {

		ProtoParseMsg(conn)
	}
}
