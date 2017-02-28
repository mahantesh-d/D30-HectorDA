package presto

import (
	"database/sql"
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/model"
	"github.com/dminGod/D30-HectorDA/utils"
	"time"
	"github.com/dminGod/D30-HectorDA/endpoint/cassandra_helper"
	"github.com/dminGod/D30-HectorDA/endpoint/presto_helper"
	_ "github.com/avct/prestgo" // SQL Driver uses prestgo

)

var prestoChan chan *sql.DB



func init() {

	prestoChan = make(chan *sql.DB, 100)
}

// Handle acts as an entry point to handle different operations on Presto
func Handle(dbAbstract *model.DBAbstract) {

	if dbAbstract.QueryType == "SELECT" {

		Select(dbAbstract)
	}
}

func getSession() (*sql.DB, error) {

	Conf := config.Get()
	logger.Write("INFO", "Initializing Presto Session")
	select {

	case prestoSession := <-prestoChan:
		logger.Write("INFO", "Using Existing Presto session")
		return prestoSession, nil

	case <-time.After(100 * time.Millisecond):
		logger.Write("INFO", "Creating new Presto Connection")

		db, err := sql.Open("prestgo", Conf.Presto.ConnectionURL)

		if err != nil {
			logger.Write("ERROR", "Could not connect: " + err.Error())	
		}

		defer queueSession(db)

		return db, nil
	}
}

// Select is used to query data from presto
func Select(dbAbstract *model.DBAbstract) {

	var prestoResult []map[string]interface{}

	if len(dbAbstract.Query) == 0 {

		logger.Write("ERROR", "Presto received a blank query to process in the DBAbstract object")
		dbAbstract.Status = "fail"
		dbAbstract.Message = "Presto received a blank query to process in the DBAbstract object"
		dbAbstract.Data = "{}"
		dbAbstract.Count = 0
		return
	}

	session, _ := getSession()
	logger.Write("DEBUG", "QUERY : " + dbAbstract.Query[0])
	rows, err := session.Query(dbAbstract.Query[0])


	if err != nil {
		panic(err)
	}

	cols, err := rows.Columns()

	data := make([]interface{}, len(cols))
	args := make([]interface{}, len(data))

	for i := range data {
		args[i] = &data[i]
	}

	for rows.Next() {

		if err := rows.Scan(args...); err != nil {
			panic(err.Error())
		}

		for i := range data {

			prestoResult = append(prestoResult, map[string]interface{}{cols[i]: data[i]})
		}
	}

	if err != nil {

		logger.Write("ERROR", err.Error())
		dbAbstract.Status = "fail"
		dbAbstract.Message = err.Error()
		dbAbstract.Data = "{}"
		dbAbstract.Count = 0
	} else {

		dbAbstract.Status = "success"
		dbAbstract.Message = "Select successful"
		dbAbstract.Data = utils.EncodeJSON(prestoResult)
		dbAbstract.Count = uint64(len(prestoResult))
		dbAbstract.RichData = prestoResult
	}
}


func QueryPrestoMakeCassandraInQuery( metaResult map[string]interface{}, metaInput map[string]interface{} ) string {

	metaResult["databaseType"] = "presto"
	query := []string{presto_helper.FindIDQueryBuild(metaInput)}


	var dbAbsPresto model.DBAbstract

	dbAbsPresto.QueryType = "SELECT"
	dbAbsPresto.Query = query
	dbAbsPresto.DBType = "presto"

	Select(&dbAbsPresto)

	cassandraInQuery := cassandra_helper.MakeCassandraInQuery(dbAbsPresto.RichData, metaInput)

	return cassandraInQuery
}





func queueSession(session *sql.DB) {

	select {
	case prestoChan <- session:
		// session enqueued
	default:
		logger.Write("INFO", "Channel full")
		session.Close()
	}
}
