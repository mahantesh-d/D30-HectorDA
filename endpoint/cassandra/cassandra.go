package cassandra

import (
	"fmt"
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/model"
	"github.com/dminGod/D30-HectorDA/utils"
	"github.com/gocql/gocql"
	"strings"
	"time"
)

var cassandraChan chan *gocql.Session
var cassandraSession *gocql.Session
var cassandraHost []string

func init() {
	cassandraChan = make(chan *gocql.Session, 500)
}

// Handle acts as an entry point to handle different operations on Cassandra
func Handle(dbAbstract *model.DBAbstract) {

	Conf := config.Get()

	cassandraHost = Conf.Cassandra.Host

	if dbAbstract.QueryType == "INSERT" {

		Insert(dbAbstract)
	} else if dbAbstract.QueryType == "SELECT" {

		Select(dbAbstract)
	}
}

func getSession() (*gocql.Session, error) {
	logger.Write("INFO", "Initializing Cassandra Session")
	select {

	case cassandraSession := <-cassandraChan:
		logger.Write("INFO", "Using Existing session")
		return cassandraSession, nil
	case <-time.After(100 * time.Millisecond):
		logger.Write("INFO", "Creating new Cassandra Connection")

		cluster := gocql.NewCluster(cassandraHost...)
		cluster.Keyspace = "system"
		cluster.ProtoVersion = 3
		session, err := cluster.CreateSession()

		utils.HandleError(err)	
		return session, err
	}
}

// Insert is used to make an Insert into Cassandra
func Insert(dbAbstract *model.DBAbstract) {

	session, err := getSession()
	if err != nil {
        	logger.Write("ERROR", err.Error())
        	dbAbstract.Status = "fail"
        	dbAbstract.Message = "Error connecting to endpoint"
        	dbAbstract.Data = "{}"
        	dbAbstract.Count = 0
         	return
 	}
	logger.Write("DEBUG", "Running Queries for insert start : num of queries to run "+string(len(dbAbstract.Query)))

	success_count := 0
	var error_messages []string

	// Loop over all the queries and execute the insert queries
	for _, single_query := range dbAbstract.Query {

		logger.Write("DEBUG", "QUERY : "+dbAbstract.Query[0])
		err := session.Query(single_query).Exec()

		if err != nil {

			logger.Write("ERROR", "Query from set failed - Query : '"+single_query+"' - Error : "+err.Error())
			error_messages = append(error_messages, "Query from set failed - Query : '"+single_query+"' - Error : "+err.Error())
		} else {

			success_count += 1
		}
	}

	if len(error_messages) > 0 {

		// Error response text
		response_text := string(len(error_messages)) + " Out of " + string(len(dbAbstract.Query)) + " Had the following errors \n"
		response_text += strings.Join(error_messages, " \n")

		logger.Write("ERROR", response_text)
		dbAbstract.Status = "fail"
		dbAbstract.Message = response_text
		dbAbstract.Data = "{}"
	} else {

		logger.Write("INFO", "Inserted successfully")
		dbAbstract.Status = "success"
		dbAbstract.Message = "Inserted successfully"
		dbAbstract.Data = "{}"
	}

	dbAbstract.Count = 0

	go queueSession(session)

}

// Select is used to query data from Cassandra
func Select(dbAbstract *model.DBAbstract) {

	if len(dbAbstract.Query) == 0 {
		dbAbstract.Status = "fail"
		dbAbstract.Message = "Invalid Query"
		dbAbstract.Data = "{}"
		dbAbstract.Count = 0
		return
	}

	session, err := getSession()
	
	if err != nil {
        	logger.Write("ERROR", err.Error())
        	dbAbstract.Status = "fail"
        	dbAbstract.Message = "Error connecting to endpoint"
        	dbAbstract.Data = "{}"
        	dbAbstract.Count = 0
		return
	}

	// Currently only single queries for select are supported.
	// The query field is an []string so we are using the 0 element on it

	logger.Write("DEBUG", "QUERY : "+dbAbstract.Query[0])

	iter := session.Query(dbAbstract.Query[0]).Iter()
	result, err := iter.SliceMap()

	fmt.Println("Running the cassandra select query : " + dbAbstract.Query[0])

	_ = err
	if err != nil {
		logger.Write("ERROR", err.Error())
		dbAbstract.Status = "fail"
		dbAbstract.Message = err.Error()
		dbAbstract.Data = "{}"
		dbAbstract.Count = 0
	} else {
		dbAbstract.Status = "success"
		dbAbstract.Message = "Query successful"
		data := utils.EncodeJSON(result)
		dbAbstract.Data = data
		dbAbstract.Count = uint64(len(result))
	}
}

func queueSession(session *gocql.Session) {

	select {
	case cassandraChan <- session:
		// session enqueued
	default:
		logger.Write("INFO", "Channel full")
		session.Close()
	}

}
