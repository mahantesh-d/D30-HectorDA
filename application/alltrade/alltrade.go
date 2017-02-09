package alltrade

import(
	"github.com/dminGod/D30-HectorDA/model"
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/endpoint"
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/utils"
	"github.com/dminGod/D30-HectorDA/metadata"
	"github.com/dminGod/D30-HectorDA/lib/queryhelper"
	_"strings"
)

var conf config.Config 
var metaData map[string]interface{}
func init() {
	conf = config.Get()
	metaData = utils.DecodeJSON(utils.ReadFile("/etc/hector/metadata/alltrade/alltrade.json"))
}

func Foo_Post(req model.RequestAbstract) (model.ResponseAbstract) {

	logger.Write("DEBUG", "Function Foo_Post Executing...")


	var dbAbs model.DBAbstract
	dbAbs.DBType = "cassandra"
	dbAbs.QueryType = "INSERT"
	dbAbs.Query = utils.PrepareInsert("foo", req.Payload) 

	endpoint.Process(nil,&conf, &dbAbs)

	
	var responseAbstract model.ResponseAbstract
	if dbAbs.Status == "fail" {
		logger.Write("ERROR", dbAbs.Message)
		responseAbstract.StatusCode = 500
	} else {
		responseAbstract.StatusCode = 200
	}
	responseAbstract.Status = dbAbs.Status	
	responseAbstract.StandardStatusMessage =  dbAbs.StatusCodeMessage
	responseAbstract.Text = dbAbs.Message
	responseAbstract.Data = dbAbs.Data
	responseAbstract.Count = dbAbs.Count
	
	return responseAbstract
}


func Foo_Get(req model.RequestAbstract) (model.ResponseAbstract) {
	
	logger.Write("DEBUG", "Function Foo_Get Executing...")
	var dbAbs model.DBAbstract

	dbAbs.DBType = "cassandra"
	dbAbs.QueryType = "SELECT"
	dbAbs.Query = "SELECT * from foo"
	endpoint.Process(nil,&conf,&dbAbs)

	var responseAbstract model.ResponseAbstract
	if dbAbs.Status == "fail" {
        	logger.Write("ERROR", dbAbs.Message)
         	responseAbstract.StatusCode = 500
	} else {
        	responseAbstract.StatusCode = 200
 	}
	responseAbstract.Status = dbAbs.Status
	responseAbstract.StandardStatusMessage =  dbAbs.StatusCodeMessage
	responseAbstract.Text = dbAbs.Message
	responseAbstract.Data = dbAbs.Data
	responseAbstract.Count = dbAbs.Count

	return responseAbstract
}

func StockAdjustment_Post(req model.RequestAbstract) (model.ResponseAbstract) {
	
	logger.Write("DEBUG", "Function Foo_Get Executing...")
	var dbAbs model.DBAbstract
	dbAbs.DBType = "cassandra"
	dbAbs.QueryType = "INSERT"

	//endpoint.Process(nil,&conf,&dbAbs)

	var responseAbstract model.ResponseAbstract
	if dbAbs.Status == "fail" {
        	logger.Write("ERROR", dbAbs.Message)
        	responseAbstract.StatusCode = 500
 	} else {
        	responseAbstract.StatusCode = 200
 	}
	responseAbstract.Status = dbAbs.Status
 	responseAbstract.StandardStatusMessage =  dbAbs.StatusCodeMessage
 	responseAbstract.Text = dbAbs.Message
 	responseAbstract.Data = dbAbs.Data
 	responseAbstract.Count = dbAbs.Count

	
	return responseAbstract
}


func Foobar_Post(req model.RequestAbstract) (model.ResponseAbstract) {

	metaInput := utils.FindMap("table","foobar", metaData)
	metaResult := metadata.Interpret(metaInput, req.Payload)
	query := queryhelper.PrepareQuery(metaResult)

	var dbAbs model.DBAbstract
	dbAbs.DBType = "cassandra"
	dbAbs.QueryType = "INSERT"
	dbAbs.Query = query
	endpoint.Process(nil,&conf,&dbAbs)

	return prepareResponse(dbAbs)
}

func prepareResponse(dbAbs model.DBAbstract) model.ResponseAbstract {
	
	var responseAbstract model.ResponseAbstract
	responseAbstract.Status = dbAbs.Status
	responseAbstract.StandardStatusMessage =  dbAbs.StatusCodeMessage
	responseAbstract.Text = dbAbs.Message
	responseAbstract.Data = dbAbs.Data
	responseAbstract.Count = dbAbs.Count

	return responseAbstract
}
