package endpoint

import (
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/endpoint/cassandra"
	"github.com/dminGod/D30-HectorDA/model"
	"net"
)

func Process(Conn *net.Conn, Conf *config.Config, HectorSession *model.HectorSession) {

	//logger.Write("INFO", "Processing Message", *Conf.Hector.Log)
	//logger.Write("INFO", "Target Endpoint is " + *HectorSession.GetEndpoint(), *Conf.Hector.Log)

	endpoint := HectorSession.Endpoint
	if endpoint == "cassandra" {
		//cassandra.Handle(Conn, Conf, HectorSession)
		cassandra.Handle()
	} else {

	}
}
