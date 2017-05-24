package queryhelper

import (
	"github.com/dminGod/D30-HectorDA/endpoint/cassandra_helper"
	"github.com/dminGod/D30-HectorDA/endpoint/presto_helper"
	"github.com/dminGod/D30-HectorDA/endpoint/postgresql_helper"

	//"google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

// PrepareInsertQuery is used to parse Application metadata
// and return the corresponding INSERT query
func PrepareInsertQuery(metaInput map[string]interface{}) []string {

	// get the endpoint
	databaseType := metaInput["databaseType"].(string)

	var query []string

	if databaseType == "cassandra" {

		query = cassandra_helper.InsertQueryBuild(metaInput)
	}else if databaseType == "postpresto" {

	}else if databaseType == "postgresxl"{

                query =postgresql_helper.InsertQueryBuild(metaInput)
	}
	return query
}

// PrepareSelectQuery is used to parse Application metadata
// and return the corresponding SELECT query
func PrepareSelectQuery(metaInput map[string]interface{}) []string {
	// get the endpoint
	databaseType := metaInput["databaseType"].(string)


	var query []string

	if databaseType == "cassandra" {

		query = []string{cassandra_helper.SelectQueryBuild(metaInput)}
	} else if databaseType == "presto" {

		query = []string{presto_helper.FindIDQueryBuild(metaInput)}
	} else if databaseType == "cassandra_stratio" {

		query = []string{cassandra_helper.StratioSelectQueryBuild(metaInput)}
	} else if databaseType == "postgresxl"{

		query =[]string{postgresql_helper.SelectQueryBuild(metaInput)}
	}

	return query
}

func PrepareUpdateQuery(metaInput map[string]interface{}) []string  {

	databaseType := metaInput["databaseType"].(string)
        // databaseType:="postgresxl"

	var query []string

	if databaseType == "cassandra" {

		query = cassandra_helper.UpdateQueryBuilder(metaInput)
	} else if databaseType == "presto" {


	} else if databaseType == "cassandra_stratio" {

	}else if databaseType =="postgresxl"{

		query = postgresql_helper.UpdateQueryBuilder(metaInput)
	}

	return query
}

func PrepareDeleteQuery(metaInput map[string]interface{}) []string {
	//databaseType := metaInput["databaseType"].(string)
      databaseType:="postgresxl"
	var query []string
	if databaseType == "cassandra" {

	} else if databaseType == "presto" {

	} else if databaseType == "cassandra_stratio" {

	} else if databaseType == "postgresxl" {

		query = []string{postgresql_helper.DeleteQueryBuilder(metaInput)}
	}

	return query
}