package presto

import (
	"github.com/gocql/gocql"
//	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/model"
	"github.com/dminGod/D30-HectorDA/logger"
	"net"
	"time"	
	_"strings"
	_"strconv"
	"github.com/dminGod/D30-HectorDA/utils"



	"database/sql"
//	"fmt"
//	"log"

	_ "github.com/avct/prestgo"
//	"os"
)

var prestoChan chan *sql.DB

var prestoResult []map[string]interface{}

func init() {

	prestoChan = make(chan *sql.DB, 100)
}

func Handle( dbAbstract *model.DBAbstract ) {

	if(dbAbstract.QueryType == "SELECT") {

		Select( dbAbstract )
	}
}

func getSession() (*sql.DB, error) {

	logger.Write("INFO", "Initializing Presto Session")
	select {

     		case prestoSession := <-prestoChan:
			logger.Write("INFO", "Using Existing Presto session")
                	return  prestoSession, nil

         	case <-time.After(100 * time.Millisecond):
                	logger.Write("INFO", "Creating new Presto Connection")

			// TODO: Make the server config dynanmic
			db, err := sql.Open("prestgo", "presto://10.138.32.26:8080/cassandra/ais_test_all")

                 	if err != nil {
                        	panic(err)
                 	}

			defer queueSession(db)

                 	return db, nil
	}
}

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
