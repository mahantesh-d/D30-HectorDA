package cassandra

import (
	"github.com/gocql/gocql"
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/model"
	"github.com/dminGod/D30-HectorDA/logger"
	"net"
	"time"	
	"strings"
	"strconv"
)

var cassandraChan chan *gocql.Session
var cassandraSession *gocql.Session
var cassandraHost string

func init() {
	cassandraChan = make(chan *gocql.Session,100)
}
func Handle(Conn *net.Conn, Conf *config.Config, HectorSession *model.HectorSession) (model.HectorResponse) {

	var response model.HectorResponse
	cassandraHost = Conf.Cassandra.Host
	if HectorSession.Method == "POST" {
		query := preparePost(HectorSession)
		response = post(query)
	}

	return response

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

func preparePost(HectorSession *model.HectorSession) string {

	module := HectorSession.Module
	query := "INSERT INTO " + module

	name := " ( "
	value := " ( "
	for i,v := range HectorSession.Payload {
	
		name += (i + ",")
		switch c := v.(type) {
		
			case string:
				val := "'" + v.(string) + "'"
				value += val
			case int32, int64:
				
				val := (strconv.Itoa(v.(int)))
				value += val
			case float32,float64:
				val := strconv.FormatFloat(v.(float64),'f',-1,64)
 				value += val
			default:
				_ = c
		}
		
		value += ","
	}	

	name = strings.Trim(name,",")
	value = strings.Trim(value,",")

	query += name + " ) VALUES " + value + " ) "

	
	return query
}


func post(query string) (model.HectorResponse) {
	
	session,_ := getSession()
	logger.Write("DEBUG", "QUERY : " + query)
	err := session.Query(query).Exec() 
	var response model.HectorResponse
	if err != nil {
		logger.Write("ERROR", err.Error())
		response.Status = "fail"
		response.Message = err.Error()
		response.Data = "{}"
	} else {
		logger.Write("INFO","Inserted successfully")
		response.Status = "success"
		response.Message = "Inserted successfully"
		response.Data = "{}"
	}
	go queueSession(session)

	return response
}

func get(query string) {

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
