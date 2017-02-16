package cassandra

import (
	"github.com/gocql/gocql"
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/model"
	"github.com/dminGod/D30-HectorDA/logger"
	"net"
	"time"	
	"github.com/dminGod/D30-HectorDA/utils"
)

var cassandraChan chan *gocql.Session
var cassandraSession *gocql.Session
var cassandraHost string

func init() {
	cassandraChan = make(chan *gocql.Session,100)
}

// Handle acts as an entry point to handle different operations on Cassandra
func Handle(Conn *net.Conn, Conf *config.Config, dbAbstract *model.DBAbstract) {

	cassandraHost = Conf.Cassandra.Host
	
	if(dbAbstract.QueryType == "INSERT") {
		Insert(dbAbstract)
	} else if(dbAbstract.QueryType == "SELECT") {
		Select(dbAbstract)
	}
}

func getSession() (*gocql.Session,error) {
	logger.Write("INFO","Initializing Cassandra Session")
	select {

     		case cassandraSession := <-cassandraChan:
			logger.Write("INFO", "Using Existing session")
                	return  cassandraSession,nil
         	case <-time.After(100 * time.Millisecond):
                	logger.Write("INFO", "Creating new Cassandra Connection")
			cluster := gocql.NewCluster(cassandraHost)
                 	cluster.Keyspace = "all_trade"
                 	cluster.ProtoVersion = 3
                	session, err := cluster.CreateSession()

                 	if err != nil {
                        	panic(err)
                 	}

                 	return session,nil
	}
}

// Insert is used to make an Insert into Cassandra
func Insert(dbAbstract *model.DBAbstract) {
	
	session,_ := getSession()
	logger.Write("DEBUG", "QUERY : " + dbAbstract.Query)
	err := session.Query(dbAbstract.Query).Exec() 
	if err != nil {
		logger.Write("ERROR", err.Error())
		dbAbstract.Status = "fail"
		dbAbstract.Message = err.Error()
		dbAbstract.Data = "{}"
	} else {
		logger.Write("INFO","Inserted successfully")
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

	session,_ := getSession()
	
	logger.Write("DEBUG", "QUERY : " + dbAbstract.Query)
	iter := session.Query(dbAbstract.Query).Iter()
	result,err := iter.SliceMap()
	
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
		data  := utils.EncodeJSON(result)
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
