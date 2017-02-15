package alltrade

import(
	_"fmt"
	"github.com/dminGod/D30-HectorDA/model"
//	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/endpoint"
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/utils"
	"github.com/dminGod/D30-HectorDA/metadata"
	"github.com/dminGod/D30-HectorDA/lib/queryhelper"
	"strings"
)

var conf config.Config 
var metaData map[string]interface{}
var metaDataSelect map[string]interface{}
func init() {
	conf = config.Get()
	metaData = utils.DecodeJSON(utils.ReadFile("/etc/hector/metadata/alltrade/alltrade.json"))
	metaDataSelect = utils.DecodeJSON(utils.ReadFile("/etc/hector/metadata/alltrade/alltradeApi.json"))
}

// This is is an API to adjust Stock, it accepts POST params

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


// This is is an API to adjust Stock, it accepts GET params

func StockAdjustment_Get(req model.RequestAbstract) (model.ResponseAbstract) {

	// input: map of queryparams key and value and metadata
	// output: map of queryparams key and values+tablemetadata
	metaDataSelect = utils.DecodeJSON(utils.ReadFile("/etc/hector/metadata/alltrade/alltradeApi.json"))
	metaInput := utils.FindMap("table","stock_adjustment", metaDataSelect)

	metaResult := metadata.InterpretSelect(metaInput,req.Filters)

	query := ""	

	var dbAbs model.DBAbstract
	if queryhelper.IsValidCassandraQuery(metaResult) {
		query = queryhelper.PrepareSelectQuery(metaResult)
		dbAbs.DBType = "cassandra"
		dbAbs.QueryType = "SELECT"
		dbAbs.Query = query
	} else {
		metaResult["databaseType"] = "presto"
		query = queryhelper.PrepareSelectQuery(metaResult)
	}


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

func ObtainDetail_Get(req model.RequestAbstract) (model.ResponseAbstract) {


	 // input: map of queryparams key and value and metadata
	// output: map of queryparams key and values+tablemetadata
	metaDataSelect = utils.DecodeJSON(utils.ReadFile("/etc/hector/metadata/alltrade/alltradeApi.json"))
	metaInput := utils.FindMap("table","obtain_detail", metaDataSelect)

	metaResult := metadata.InterpretSelect(metaInput,req.Filters)


	query := ""

 	var dbAbs model.DBAbstract
	if queryhelper.IsValidCassandraQuery(metaResult) {
        	query = queryhelper.PrepareSelectQuery(metaResult)
        	dbAbs.DBType = "cassandra"
        	dbAbs.QueryType = "SELECT"
         	dbAbs.Query = query
	 } else {
        	metaResult["databaseType"] = "presto"
         	query = queryhelper.PrepareSelectQuery(metaResult)
	}	

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

func SubStockDetailTransfer_Get(req model.RequestAbstract) (model.ResponseAbstract) {

	// input: map of queryparams key and value and metadata
	// output: map of queryparams key and values+tablemetadata
 	metaDataSelect = utils.DecodeJSON(utils.ReadFile("/etc/hector/metadata/alltrade/alltradeApi.json"))
 	metaInput := utils.FindMap("table","sub_stock_detail_transfer", metaDataSelect)
	metaResult := metadata.InterpretSelect(metaInput,req.Filters)


	 query := ""

	var dbAbs model.DBAbstract
	if queryhelper.IsValidCassandraQuery(metaResult) {
        	query = queryhelper.PrepareSelectQuery(metaResult)
        	dbAbs.DBType = "cassandra"
        	dbAbs.QueryType = "SELECT"
        	dbAbs.Query = query
 	} else {
        	metaResult["databaseType"] = "presto"
        	query = queryhelper.PrepareSelectQuery(metaResult)
 	}

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

func ReportsRequestGood_Get(req model.RequestAbstract) (model.ResponseAbstract) {

	query := `
	SELECT to_location_name, 
	to_location_code, 
	ship_to_province, 
	ship_to_code, 
	reserved_no, 
	request_no, 
	remark text, 
	receive_by, 
	picking_datetime, 
	model_key, 
	mobile_no, 
	for_substock, 
	do_no, 
	create_datetime, 
	company, 
	commercial_name_key
	FROM request_goods
	`
	if len(req.Filters) > 0 {
		
		query += " WHERE "

		for k,v := range req.Filters {
			
			if (k == "transactionType") {
				query += (" transaction_type = '"+ v + "' AND")
			} else if k == "fromLocationSubType" {
				query += (" from_location_subtype = '" + v + "' AND")
			} else if k == "fromLocationType" {
				query += (" from_location_type = '" + v + "' AND")
			} else if k == "requestStatus" {
				query += (" request_status = '" + v + "' AND")
			} else if k == "company" {
				query += (" company IN (")
			
				values := strings.Split(v,",")
	
				for _,val := range values {

					query += "'" + val + "' ,"
				}
				query = strings.Trim(query,",")
				query += ") AND"
			} else if k == "fromLocationCode" {

				query += (" from_location_code IN (")

				values := strings.Split(v,",")

				for _,val := range values {

        				query += "'" + val + "' ,"
				}
				query = strings.Trim(query,",")
				query += ") AND"

			} else if k == "createDateTimeRange" {

				values := strings.Split(v,",")
	
				if len(values) == 2 {

					query += (" create_datetime BETWEEN timestamp '" + values[0] + "' AND timestamp '" + values[1] + "'")
					query += " AND"
				}

			}

		}

	}

	query = strings.Trim(query,"AND")	
	var dbAbs model.DBAbstract
	dbAbs.DBType = "presto"
	dbAbs.QueryType = "SELECT"
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
