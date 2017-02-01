package endpoint

import(
	"net"
	"github.com/dminGod/D30-HectorDA/model"
	"github.com/dminGod/D30-HectorDA/endpoint/cassandra"
)

func Process(Conn *net.Conn, Conf *model.Config, HectorSession *model.HectorSession) {

	//logger.Write("INFO", "Processing Message", *Conf.Hector.Log)
	//logger.Write("INFO", "Target Endpoint is " + *HectorSession.GetEndpoint(), *Conf.Hector.Log)

	endpoint := HectorSession.Endpoint
	if endpoint == "cassandra" {
		//cassandra.Handle(Conn, Conf, HectorSession)
		cassandra.Handle()
	} else {
	
	}
}
