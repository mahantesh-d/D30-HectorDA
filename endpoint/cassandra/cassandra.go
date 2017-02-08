package cassandra

import (
	"github.com/gocql/gocql"
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/model"
	"github.com/dminGod/D30-HectorDA/logger"
	"net"
	"time"	
	_"strings"
	_"strconv"
	"github.com/dminGod/D30-HectorDA/utils"
)

var cassandraChan chan *gocql.Session
var cassandraSession *gocql.Session
var cassandraHost string

func init() {
	cassandraChan = make(chan *gocql.Session,100)
}

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

func Select(dbAbstract *model.DBAbstract) {

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
		dbAbstract.Message = "Inserted successfully"
		dbAbstract.Data = utils.EncodeJSON(result)
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
