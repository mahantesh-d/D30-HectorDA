package servers

import (
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/model"
	"github.com/spf13/viper"
	"net"
)

var serverType string

var Conf config.Config
var HectorSession model.HectorSession

// Used to start a TCP server
func Server(serverTypePassed string) {

	// Set the server Type
	serverType = serverTypePassed

	Conf.Hector.Host = viper.GetString("hector.host")
	Conf.Hector.Log = viper.GetString("hector.log")
	Conf.Hector.Port = viper.GetString("hector.port")
	Conf.Hector.ConnectionType = viper.GetString("hector.connectiontype")

	// listen to the TCP port
	logger.Write("INFO", "Starting Server on "+Conf.Hector.Host+":"+Conf.Hector.Port, Conf.Hector.Log)
	listener, _ := net.Listen(Conf.Hector.ConnectionType, Conf.Hector.Host+":"+Conf.Hector.Port)
	logger.Write("INFO", "==== Server Running on "+Conf.Hector.Host+":"+Conf.Hector.Port+" =======", Conf.Hector.Log)

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
