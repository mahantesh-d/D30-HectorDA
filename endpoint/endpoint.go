package endpoint

import (
	"github.com/dminGod/D30-HectorDA/endpoint/cassandra"
	"github.com/dminGod/D30-HectorDA/endpoint/presto"
	"github.com/dminGod/D30-HectorDA/model"
	"github.com/dminGod/D30-HectorDA/endpoint/postgresql"
)

// Process acts as an entry point to mapping the different data operations to different database endpoints
func Process(DBAbstract *model.DBAbstract) {

	endpoint := DBAbstract.DBType

	if endpoint == "cassandra" || endpoint == "cassandra_stratio" {

		cassandra.Handle(DBAbstract)
	} else if endpoint == "presto" {

		presto.Handle(DBAbstract)
	} else if endpoint == "postgresxl"{

		postgresxl.Handle(DBAbstract)
	}
}
