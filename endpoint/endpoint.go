package endpoint

import (
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/endpoint/cassandra"
	"github.com/dminGod/D30-HectorDA/model"
	"net"
)

func Process(Conn *net.Conn, Conf *config.Config, HectorSession *model.HectorSession) (model.HectorResponse) {

	endpoint := HectorSession.Endpoint

	var output model.HectorResponse

	if endpoint == "cassandra" {
		output = cassandra.Handle(Conn, Conf, HectorSession)
	} else {

	}

	return output

}

func Em() {

}
