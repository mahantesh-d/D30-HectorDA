package alltrade

import (
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/constant"
	"github.com/dminGod/D30-HectorDA/endpoint"
	"github.com/dminGod/D30-HectorDA/lib/queryhelper"
	_ "github.com/dminGod/D30-HectorDA/logger" // TODO: Add Intermdiate logs to track detailed activity of each API
	"github.com/dminGod/D30-HectorDA/metadata"
	"github.com/dminGod/D30-HectorDA/model"
	"github.com/dminGod/D30-HectorDA/utils"
	"strings"
	"github.com/dminGod/D30-HectorDA/endpoint/cassandra_helper"
	"github.com/dminGod/D30-HectorDA/endpoint/presto"
)

var conf config.Config
var metaData map[string]interface{}
var metaDataSelect map[string]interface{}

func init() {
	conf = config.Get()
	metaData = utils.DecodeJSON(utils.ReadFile(constant.HectorConf + "/metadata/alltrade/alltrade.json"))
	metaDataSelect = utils.DecodeJSON(utils.ReadFile(constant.HectorConf + "/metadata/alltrade/alltradeApi.json"))
}

func ReturnRoutes() (map[string]func(model.RequestAbstract) model.ResponseAbstract){

	routes := map[string]func(model.RequestAbstract) model.ResponseAbstract{

		// All trade version 1
		"alltrade_stock_adjustment_post": StockAdjustmentPost,
		"alltrade_stock_adjustment_get":  StockAdjustmentGet,

		"alltrade_obtain_detail_post": ObtainDetailPost,
		"alltrade_obtain_detail_get":  ObtainDetailGet,

		"alltrade_substock_detail_transfer_post": SubStockDetailTransferPost,
		"alltrade_substock_detail_transfer_get":  SubStockDetailTransferGet,

		"alltrade_substock_daily_detail_post": SubStockDailyDetailPost,
		"alltrade_substock_daily_detail_get":  SubStockDailyDetailGet,

		"alltrade_transferout_mismatch_post": TransferOutMismatchPost,
		"alltrade_transferout_mismatch_get":  TransferOutMismatchGet,

		"alltrade_requestgoods_post": RequestGoodsPost,
		"alltrade_requestgoods_get":  RequestGoodsGet,

		"alltrade_ordertransfer_post": OrderTransferPost,
		"alltrade_ordertransfer_get":  OrderTransferGet,

		"alltrade_saleout_detail_post": SaleOutDetailPost,
		"alltrade_saleout_detail_get":  SaleOutDetailGet,

		"alltrade_checkstock_detail_post": CheckStockDetailPost,
		"alltrade_checkstock_detail_get":  CheckStockDetailGet,

		"alltrade_reports_requestgoods_get": ReportsRequestGoodGet}

	return routes

}

func EnrichRequest(reqAbs *model.RequestAbstract) {

	// Cassandra Query Type Decision:

//	if(reqAbs.HTTPRequestType == "GET") {

		// One of the 3 options will be returned on this :
		// "single_column", "multi_column_same_index", "multi_column_mixed_index"

		//_, ok := reqAbs.AdditionalData["cassandra_query_type"]
		//
		//if !ok {
		//
		//	fmt.Println("going in to not okay")
		//	os.Exit(1)
		//	// reqAbs.AdditionalData["cassandra_query_type"]
		//}

		// reqAbs.AdditionalData["cassandra_query_type"] = cassandra_helper.DecideQueryTypeByRequest(reqAbs, metaDataSelect)
//	}
}

func EnrichResponse(reqAbs *model.ResponseAbstract) {


}

// StockAdjustmentPost handles StockAdjustment POST request

func StockAdjustmentPost(req model.RequestAbstract) model.ResponseAbstract {
	metaInput := utils.FindMap("table", "stock_adjustment", metaData)
	metaResult := metadata.Interpret(metaInput, req.Payload)
	query := queryhelper.PrepareInsertQuery(metaResult)

	var dbAbs model.DBAbstract
	dbAbs.DBType = "cassandra"
	dbAbs.QueryType = "INSERT"
	dbAbs.Query = query
	endpoint.Process(&dbAbs)

	return prepareResponse(dbAbs)
}

// StockAdjustmentGet handles StockAdjustment GET request

func StockAdjustmentGet(req model.RequestAbstract) model.ResponseAbstract {

	metaDataSelect = utils.DecodeJSON(utils.ReadFile(constant.HectorConf + "/metadata/alltrade/alltradeApi.json"))
	metaInput := utils.FindMap("table", "stock_adjustment", metaDataSelect)
	metaResult := metadata.InterpretSelect(metaInput, req.Filters)

	var query []string

	var dbAbs model.DBAbstract

	dbAbs.QueryType = "SELECT"
	dbAbs.DBType = "cassandra"

	if cassandra_helper.IsValidCassandraQuery(metaResult) {

		query = queryhelper.PrepareSelectQuery(metaResult)
	} else {

		query = []string{presto.QueryPrestoMakeCassandraInQuery( metaResult, metaInput )}
	}

	dbAbs.Query = query
	endpoint.Process(&dbAbs)

	return prepareResponse(dbAbs)
}



// ObtainDetailPost handles ObtainDetail POST request

func ObtainDetailPost(req model.RequestAbstract) model.ResponseAbstract {
	metaInput := utils.FindMap("table", "obtain_detail", metaData)
	metaResult := metadata.Interpret(metaInput, req.Payload)
	query := queryhelper.PrepareInsertQuery(metaResult)

	var dbAbs model.DBAbstract
	dbAbs.DBType = "cassandra"
	dbAbs.QueryType = "INSERT"
	dbAbs.Query = query
	endpoint.Process(&dbAbs)

	return prepareResponse(dbAbs)
}


func commonRequestProcess(req model.RequestAbstract, table_name string) model.DBAbstract {

	metaDataSelect = utils.DecodeJSON(utils.ReadFile(constant.HectorConf + "/metadata/alltrade/alltradeApi.json"))
	metaInput := utils.FindMap("table", table_name, metaDataSelect)
	metaResult := metadata.InterpretSelect(metaInput, req.Filters)

	var query []string

	var dbAbs model.DBAbstract

	dbAbs.QueryType = "SELECT"
	dbAbs.DBType = "cassandra"

	if cassandra_helper.IsValidCassandraQuery(metaResult) {

		query = queryhelper.PrepareSelectQuery(metaResult)

	} else {

		query = queryhelper.PrepareSelectQuery(metaResult)
	}

	dbAbs.Query = query
	endpoint.Process(&dbAbs)

	return dbAbs
}




// ObtainDetailGet handles ObtainDetail GET request

func ObtainDetailGet(req model.RequestAbstract) model.ResponseAbstract {

	dbAbs := commonRequestProcess( req, "obtain_detail" )

	return prepareResponse(dbAbs)
}

// SubStockDetailTransferPost handles SubStockDetailTransfer POST request

func SubStockDetailTransferPost(req model.RequestAbstract) model.ResponseAbstract {
	metaInput := utils.FindMap("table", "sub_stock_detail_transfer", metaData)
	metaResult := metadata.Interpret(metaInput, req.Payload)
	query := queryhelper.PrepareInsertQuery(metaResult)

	var dbAbs model.DBAbstract
	dbAbs.DBType = "cassandra"
	dbAbs.QueryType = "INSERT"
	dbAbs.Query = query
	endpoint.Process(&dbAbs)

	return prepareResponse(dbAbs)
}

// SubStockDetailTransferGet handles SubStockDetailTransfer GET request

func SubStockDetailTransferGet(req model.RequestAbstract) model.ResponseAbstract {

	dbAbs := commonRequestProcess( req, "sub_stock_detail_transfer" )

	return prepareResponse(dbAbs)
}

// SubStockDailyDetailPost handles SubStockDailyDetail POST request

func SubStockDailyDetailPost(req model.RequestAbstract) model.ResponseAbstract {
	metaInput := utils.FindMap("table", "sub_stock_daily_detail", metaData)
	metaResult := metadata.Interpret(metaInput, req.Payload)
	query := queryhelper.PrepareInsertQuery(metaResult)

	var dbAbs model.DBAbstract
	dbAbs.DBType = "cassandra"
	dbAbs.QueryType = "INSERT"
	dbAbs.Query = query
	endpoint.Process(&dbAbs)

	return prepareResponse(dbAbs)
}

// SubStockDailyDetailGet handles SubStockDailyDetail GET request

func SubStockDailyDetailGet(req model.RequestAbstract) model.ResponseAbstract {


	dbAbs := commonRequestProcess( req, "sub_stock_daily_detail" )

	return prepareResponse(dbAbs)
}

// TransferOutMismatchPost handles TransferOutMismatch POST request

func TransferOutMismatchPost(req model.RequestAbstract) model.ResponseAbstract {
	metaInput := utils.FindMap("table", "tranfer_out_mismatch", metaData)
	metaResult := metadata.Interpret(metaInput, req.Payload)
	query := queryhelper.PrepareInsertQuery(metaResult)

	var dbAbs model.DBAbstract
	dbAbs.DBType = "cassandra"
	dbAbs.QueryType = "INSERT"
	dbAbs.Query = query
	endpoint.Process(&dbAbs)

	return prepareResponse(dbAbs)
}

// TransferOutMismatchGet handles TransferOutMismatch GET request

func TransferOutMismatchGet(req model.RequestAbstract) model.ResponseAbstract {

	dbAbs := commonRequestProcess( req, "tranfer_out_mismatch" )

	return prepareResponse(dbAbs)
}

// RequestGoodsPost handles RequestGoods POST request

func RequestGoodsPost(req model.RequestAbstract) model.ResponseAbstract {
	metaInput := utils.FindMap("table", "request_goods", metaData)
	metaResult := metadata.Interpret(metaInput, req.Payload)
	query := queryhelper.PrepareInsertQuery(metaResult)

	var dbAbs model.DBAbstract
	dbAbs.DBType = "cassandra"
	dbAbs.QueryType = "INSERT"
	dbAbs.Query = query
	endpoint.Process(&dbAbs)

	return prepareResponse(dbAbs)
}

// RequestGoodsGet handles RequestGoods GET request

func RequestGoodsGet(req model.RequestAbstract) model.ResponseAbstract {

	dbAbs := commonRequestProcess( req, "request_goods" )

	return prepareResponse(dbAbs)
}

// OrderTransferPost handles OrderTransfer POST request

func OrderTransferPost(req model.RequestAbstract) model.ResponseAbstract {

	metaInput := utils.FindMap("table", "order_transfer", metaData)
	metaResult := metadata.Interpret(metaInput, req.Payload)
	query := queryhelper.PrepareInsertQuery(metaResult)

	var dbAbs model.DBAbstract
	dbAbs.DBType = "cassandra"
	dbAbs.QueryType = "INSERT"
	dbAbs.Query = query
	endpoint.Process(&dbAbs)

	return prepareResponse(dbAbs)
}

// OrderTransferGet handles OrderTransfer GET request

func OrderTransferGet(req model.RequestAbstract) model.ResponseAbstract {


	dbAbs := commonRequestProcess( req, "order_transfer" )

	return prepareResponse(dbAbs)
}

// SaleOutDetailPost handles SaleOutDetail POST request

func SaleOutDetailPost(req model.RequestAbstract) model.ResponseAbstract {
	metaInput := utils.FindMap("table", "sale_out_detail", metaData)
	metaResult := metadata.Interpret(metaInput, req.Payload)
	query := queryhelper.PrepareInsertQuery(metaResult)

	var dbAbs model.DBAbstract
	dbAbs.DBType = "cassandra"
	dbAbs.QueryType = "INSERT"
	dbAbs.Query = query
	endpoint.Process(&dbAbs)

	return prepareResponse(dbAbs)
}

// SaleOutDetailGet handles SaleOutDetail GET request

func SaleOutDetailGet(req model.RequestAbstract) model.ResponseAbstract {

	dbAbs := commonRequestProcess( req, "sale_out_detail" )

	return prepareResponse(dbAbs)
}

// CheckStockDetailPost handles CheckStockDetail POST request

func CheckStockDetailPost(req model.RequestAbstract) model.ResponseAbstract {
	metaInput := utils.FindMap("table", "check_stock_detail", metaData)
	metaResult := metadata.Interpret(metaInput, req.Payload)
	query := queryhelper.PrepareInsertQuery(metaResult)

	var dbAbs model.DBAbstract
	dbAbs.DBType = "cassandra"
	dbAbs.QueryType = "INSERT"
	dbAbs.Query = query
	endpoint.Process(&dbAbs)

	return prepareResponse(dbAbs)
}

// CheckStockDetailGet handles CheckStockDetail GET request

func CheckStockDetailGet(req model.RequestAbstract) model.ResponseAbstract {

	metaDataSelect = utils.DecodeJSON(utils.ReadFile(constant.HectorConf + "/metadata/alltrade/alltradeApi.json"))
	metaInput := utils.FindMap("table", "check_stock_detail", metaDataSelect)
	metaResult := metadata.InterpretSelect(metaInput, req.Filters)
	var query []string

	var dbAbs model.DBAbstract
	if cassandra_helper.IsValidCassandraQuery(metaResult) {
		query = queryhelper.PrepareSelectQuery(metaResult)
		dbAbs.DBType = "cassandra"
	} else {
		metaResult["databaseType"] = "presto"
		query = queryhelper.PrepareSelectQuery(metaResult)
		dbAbs.DBType = "presto"
	}

	dbAbs.QueryType = "SELECT"
	dbAbs.Query = query
	endpoint.Process(&dbAbs)

	return prepareResponse(dbAbs)
}

// ReportsRequestGoodGet handles ReportsRequestGood GET request

func ReportsRequestGoodGet(req model.RequestAbstract) model.ResponseAbstract {

	var query_arr []string

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

		for k, v := range req.Filters {

			if k == "transactionType" {
				query += (" transaction_type = '" + v + "' AND")
			} else if k == "fromLocationSubType" {
				query += (" from_location_subtype = '" + v + "' AND")
			} else if k == "fromLocationType" {
				query += (" from_location_type = '" + v + "' AND")
			} else if k == "requestStatus" {
				query += (" request_status = '" + v + "' AND")
			} else if k == "company" {
				query += (" company IN (")

				values := strings.Split(v, ",")

				for _, val := range values {

					query += "'" + val + "' ,"
				}
				query = strings.Trim(query, ",")
				query += ") AND"
			} else if k == "fromLocationCode" {

				query += (" from_location_code IN (")

				values := strings.Split(v, ",")

				for _, val := range values {

					query += "'" + val + "' ,"
				}
				query = strings.Trim(query, ",")
				query += ") AND"

			} else if k == "createDateTimeRange" {

				values := strings.Split(v, ",")

				if len(values) == 2 {

					query += (" create_datetime BETWEEN timestamp '" + values[0] + "' AND timestamp '" + values[1] + "'")
					query += " AND"
				}

			}

		}

	}

	query = strings.Trim(query, "AND")
	var dbAbs model.DBAbstract
	dbAbs.DBType = "presto"
	dbAbs.QueryType = "SELECT"

	query_arr = append(query_arr, query)

	dbAbs.Query = query_arr
	endpoint.Process(&dbAbs)

	return prepareResponse(dbAbs)
}

func prepareResponse(dbAbs model.DBAbstract) model.ResponseAbstract {

	var responseAbstract model.ResponseAbstract
	responseAbstract.Status = dbAbs.Status
	responseAbstract.StandardStatusMessage = dbAbs.StatusCodeMessage
	responseAbstract.Text = dbAbs.Message
	responseAbstract.Data = dbAbs.Data
	responseAbstract.Count = dbAbs.Count

	return responseAbstract
}
