package alltrade

import(
	"github.com/dminGod/D30-HectorDA/model"
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/endpoint"
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/utils"
)

var conf config.Config 

func init() {
	conf = config.Get()
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
	name := ""
	value := ""
	name += "stock_adjustment_pk, adjust_date_time, adjust_status"
	value += " NOW(), " + req.Payload["adjust_datetime"].(string) + ", " + req.Payload["adjust_status"].(string)

	dbAbs.Query = "INSERT INTO stock_adjustment ( " + name + " ) VALUES ( " + value + ")"


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
