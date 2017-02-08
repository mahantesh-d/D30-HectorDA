package endpoint

import (
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/endpoint/cassandra"
	"github.com/dminGod/D30-HectorDA/model"
	"net"
)

func Process(Conn *net.Conn, Conf *config.Config, DBAbstract *model.DBAbstract) {

	endpoint := DBAbstract.DBType

	if endpoint == "cassandra" {
		cassandra.Handle(Conn, Conf, DBAbstract)
	} else {

	}

}
