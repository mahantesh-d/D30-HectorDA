package presto

import (
	"github.com/dminGod/D30-HectorDA/model"
	"github.com/dminGod/D30-HectorDA/logger"
	"time"	
	"github.com/dminGod/D30-HectorDA/utils"
	"database/sql"
	"github.com/dminGod/D30-HectorDA/config"
)


var prestoChan chan *sql.DB

var prestoResult []map[string]interface{}

func init() {

	prestoChan = make(chan *sql.DB, 100)
}

// Handle acts as an entry point to handle different operations on Presto
func Handle( dbAbstract *model.DBAbstract ) {

	if(dbAbstract.QueryType == "SELECT") {

		Select( dbAbstract )
	}
}

func getSession() (*sql.DB, error) {

	Conf := config.Get()
	logger.Write("INFO", "Initializing Presto Session")
	select {

     		case prestoSession := <-prestoChan:
			logger.Write("INFO", "Using Existing Presto session")
                	return  prestoSession, nil

         	case <-time.After(100 * time.Millisecond):
                	logger.Write("INFO", "Creating new Presto Connection")

			db, err := sql.Open("prestgo", Conf.Presto.ConnectionURL)

                 	if err != nil {
                        	panic(err)
                 	}

			defer queueSession(db)

                 	return db, nil
	}
}

// Select is used to query data from Cassandra
func Select(dbAbstract *model.DBAbstract) {

	session, _ := getSession()
	logger.Write("DEBUG", "QUERY : " + dbAbstract.Query)
	rows, err := session.Query( dbAbstract.Query )

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

			prestoResult = append(prestoResult, map[string]interface{}{ cols[i] : data[i] } )
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
		dbAbstract.Message = "Inserted successfully"
		dbAbstract.Data = utils.EncodeJSON(prestoResult)
		dbAbstract.Count = uint64(len(prestoResult))
	}
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
