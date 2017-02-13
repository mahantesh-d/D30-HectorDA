package alltrade

import(
	_"fmt"
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
var metaDataSelect map[string]interface{}
func init() {
	conf = config.Get()
	metaData = utils.DecodeJSON(utils.ReadFile("/etc/hector/metadata/alltrade/alltrade.json"))
	metaDataSelect = utils.DecodeJSON(utils.ReadFile("/etc/hector/metadata/alltrade/alltradeApi.json"))
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

	metaInput := utils.FindMap("table","stock_adjustment", metaData)
	metaResult := metadata.Interpret(metaInput, req.Payload)
	query := queryhelper.PrepareInsertQuery(metaResult)

	var dbAbs model.DBAbstract
	dbAbs.DBType = "cassandra"
	dbAbs.QueryType = "INSERT"
	dbAbs.Query = query
	endpoint.Process(nil,&conf,&dbAbs)

 	return prepareResponse(dbAbs)	
}


func StockAdjustment_Get(req model.RequestAbstract) (model.ResponseAbstract) {

	// input: map of queryparams key and value and metadata
	// output: map of queryparams key and values+tablemetadata
	metaDataSelect = utils.DecodeJSON(utils.ReadFile("/etc/hector/metadata/alltrade/alltradeApi.json"))
	metaInput := utils.FindMap("table","stock_adjustment", metaDataSelect)

	metaResult := metadata.InterpretSelect(metaInput,req.Filters)


	query := queryhelper.PrepareSelectQuery(metaResult)

	// input: map querparams key and values+tablemetadata
	// output: SQL query
	
	// endpoint process query

        var dbAbs model.DBAbstract
        dbAbs.DBType = "cassandra"
        dbAbs.QueryType = "SELECT"
	
	dbAbs.Query = query
        endpoint.Process(nil,&conf,&dbAbs)

        return prepareResponse(dbAbs)
}



func Foobar_Post(req model.RequestAbstract) (model.ResponseAbstract) {

	metaInput := utils.FindMap("table","foobar", metaData)
	metaResult := metadata.Interpret(metaInput, req.Payload)
	query := queryhelper.PrepareInsertQuery(metaResult)

	var dbAbs model.DBAbstract
	dbAbs.DBType = "cassandra"
	dbAbs.QueryType = "INSERT"
	dbAbs.Query = query
	endpoint.Process(nil,&conf,&dbAbs)

	return prepareResponse(dbAbs)
}



func ObtainDetail_Post(req model.RequestAbstract) (model.ResponseAbstract) {

        metaInput := utils.FindMap("table","obtain_detail", metaData)
	metaResult := metadata.Interpret(metaInput, req.Payload)
        query := queryhelper.PrepareInsertQuery(metaResult)

        var dbAbs model.DBAbstract
        dbAbs.DBType = "cassandra"
        dbAbs.QueryType = "INSERT"
        dbAbs.Query = query
        endpoint.Process(nil,&conf,&dbAbs)

        return prepareResponse(dbAbs)
}

func SubStockDetailTransfer_Post(req model.RequestAbstract) (model.ResponseAbstract) {

	metaInput := utils.FindMap("table","sub_stock_detail_transfer", metaData)
 	metaResult := metadata.Interpret(metaInput, req.Payload)
	query := queryhelper.PrepareInsertQuery(metaResult)
	
	var dbAbs model.DBAbstract
	dbAbs.DBType = "cassandra"
	dbAbs.QueryType = "INSERT"
	dbAbs.Query = query
	endpoint.Process(nil,&conf,&dbAbs)

	return prepareResponse(dbAbs)

}


func SubStockDailyDetail_Post(req model.RequestAbstract) (model.ResponseAbstract) {

        metaInput := utils.FindMap("table","sub_stock_daily_detail", metaData)
        metaResult := metadata.Interpret(metaInput, req.Payload)
        query := queryhelper.PrepareInsertQuery(metaResult)

        var dbAbs model.DBAbstract
        dbAbs.DBType = "cassandra"
        dbAbs.QueryType = "INSERT"
        dbAbs.Query = query
        endpoint.Process(nil,&conf,&dbAbs)

        return prepareResponse(dbAbs)

}


func TransferOutMismatch_Post(req model.RequestAbstract) (model.ResponseAbstract) {

        metaInput := utils.FindMap("table","tranfer_out_mismatch", metaData)
        metaResult := metadata.Interpret(metaInput, req.Payload)
        query := queryhelper.PrepareInsertQuery(metaResult)

        var dbAbs model.DBAbstract
        dbAbs.DBType = "cassandra"
        dbAbs.QueryType = "INSERT"
        dbAbs.Query = query
        endpoint.Process(nil,&conf,&dbAbs)

        return prepareResponse(dbAbs)

}




func RequestGoods_Post(req model.RequestAbstract) (model.ResponseAbstract) {

        metaInput := utils.FindMap("table","request_goods", metaData)
        metaResult := metadata.Interpret(metaInput, req.Payload)
        query := queryhelper.PrepareInsertQuery(metaResult)

        var dbAbs model.DBAbstract
        dbAbs.DBType = "cassandra"
        dbAbs.QueryType = "INSERT"
        dbAbs.Query = query
        endpoint.Process(nil,&conf,&dbAbs)

        return prepareResponse(dbAbs)

}





func OrderTransfer_Post(req model.RequestAbstract) (model.ResponseAbstract) {

        metaInput := utils.FindMap("table","order_transfer", metaData)
        metaResult := metadata.Interpret(metaInput, req.Payload)
        query := queryhelper.PrepareInsertQuery(metaResult)

        var dbAbs model.DBAbstract
        dbAbs.DBType = "cassandra"
        dbAbs.QueryType = "INSERT"
        dbAbs.Query = query
        endpoint.Process(nil,&conf,&dbAbs)

        return prepareResponse(dbAbs)

}




func SaleOutDetail_Post(req model.RequestAbstract) (model.ResponseAbstract) {

        metaInput := utils.FindMap("table","sale_out_detail", metaData)
        metaResult := metadata.Interpret(metaInput, req.Payload)
        query := queryhelper.PrepareInsertQuery(metaResult)

        var dbAbs model.DBAbstract
        dbAbs.DBType = "cassandra"
        dbAbs.QueryType = "INSERT"
        dbAbs.Query = query
        endpoint.Process(nil,&conf,&dbAbs)

        return prepareResponse(dbAbs)

}




func CheckStockDetail_Post(req model.RequestAbstract) (model.ResponseAbstract) {

        metaInput := utils.FindMap("table","check_stock_detail", metaData)
        metaResult := metadata.Interpret(metaInput, req.Payload)
        query := queryhelper.PrepareInsertQuery(metaResult)

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
