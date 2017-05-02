package alltrade

import  (
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/constant"
	"github.com/dminGod/D30-HectorDA/endpoint"
	"github.com/dminGod/D30-HectorDA/endpoint/cassandra_helper"
	// "github.com/dminGod/D30-HectorDA/endpoint/presto"
	"github.com/dminGod/D30-HectorDA/lib/queryhelper"
	_ "github.com/dminGod/D30-HectorDA/logger" // TODO: Add Intermdiate logs to track detailed activity of each API
	"github.com/dminGod/D30-HectorDA/metadata"
	"github.com/dminGod/D30-HectorDA/model"
	"github.com/dminGod/D30-HectorDA/utils"
	"strings"
)

var conf config.Config
var metaData map[string]interface{}
var metaDataSelect map[string]interface{}
var jsonFileContentsApi string = utils.ReadFile(constant.HectorConf + "/metadata/alltrade/alltradeApi.json")
var jsonFileContents string = utils.ReadFile(constant.HectorConf + "/metadata/alltrade/alltrade.json")

var jsonFileContentsPrivate string = utils.ReadFile(constant.HectorConf + "/metadata/alltrade/alltradeApi.json")

func init() {
	conf = config.Get()
	//metaData = utils.DecodeJSON(utils.ReadFile(constant.HectorConf + "/metadata/alltrade/alltrade.json"))
	//metaDataSelect = utils.DecodeJSON(utils.ReadFile(constant.HectorConf + "/metadata/alltrade/alltradeApi.json"))

	metaData = utils.DecodeJSON(jsonFileContents)

	metaDataSelect = utils.DecodeJSON( jsonFileContentsApi )
	// const metaDataSelectConst = utils.DecodeJSON( jsonFileContentsApi )

}

func returnString() string {

	if len(jsonFileContentsPrivate) > 0 {

		return jsonFileContentsPrivate
	} else {

		jsonFileContentsPrivate = utils.ReadFile(constant.HectorConf + "/metadata/alltrade/alltradeApi.json")
		return jsonFileContentsPrivate
	}
}


func ReturnRoutes() map[string]func(model.RequestAbstract) model.ResponseAbstract {

	routes := map[string]func(model.RequestAbstract) model.ResponseAbstract{

		// All trade version 1
		// Cassandra
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

		"alltrade_salesout_detail_post": SaleOutDetailPost,
		"alltrade_salesout_detail_get":  SaleOutDetailGet,

		"alltrade_checkstock_detail_post": CheckStockDetailPost,
		"alltrade_checkstock_detail_get":  CheckStockDetailGet,

		"alltrade_reports_requestgoods_get": ReportsRequestGoodGet,

		"alltrade_select_postgresxl_get": PostgresqlGet,

                "alltrade_user_location_get":UserLocationGet,
		"alltrade_user_location_post":UserLocationPost,


		// Postgres Methods
		"alltrade_product_master_get" :  ProductMasterGet,
		"alltrade_location_ship_to_get" :  LocationShipToGet,
		"alltrade_route_master_get" :   RouteMasterGet,
		"alltrade_location_mapping_plant_get" :   LocationMappingPlantGet,
		"alltrade_print_history_get" :   PrintHistoryGet,
		"alltrade_company_get" :   CompanyMasterGet,
		"alltrade_location_get" :   LocationGet,
		"alltrade_location_sub_stock_get" :   LocationSubStockGet,
		"alltrade_vendor_master_get" :   VendorMasterGet,
		"alltrade_supplier_master_get" :   SupplierMasterGet,
		"alltrade_config_route_group_get" :  ConfigRouteGroupGet,
		"alltrade_master_data_get" :   MasterDataGet,
		"alltrade_device_specification_get" :   DeviceSpecificationGet,
		"alltrade_language_get" :   LanguageGet,
		"alltrade_running_format_get" :   RunningFormatGet,
		"alltrade_last_running_get" :   LastRunningGet,
		"alltrade_map_location_route_group_get" :   MapLocationRouteGroupGet,
		"alltrade_schedule_report_get" :   ScheduleReportGet,
		"alltrade_location_relational_get" :   LocationRelationalGet,
		"alltrade_news_feed_get" :   NewsFeedGet,
		"alltrade_todo_list_get" :   TodoListGet,
		"alltrade_menu_get" :   MenuGet,
		"alltrade_asc_master_get" :   AscMasterGet,
		"alltrade_qty_per_time_interval_get" :   QtyPerTimeIntervalGet,
		"alltrade_user_login_get" :   UserLoginGet,
		"alltrade_user_authen_get" :   UserAuthenGet,
		"alltrade_user_group_get" :   UserGroupGet,
		"alltrade_user_component_get" :   UserComponentGet,
		"alltrade_user_group_location_get" :   UserGroupLocationGet,
		"alltrade_group_location_get" :  GroupLocationGet,

		"alltrade_user_component_post":UserComponentPost,
		"alltrade_update_postgresxl_post":PostgresqlUpdate,
		"alltrade_user_group_location_post":UserGroupLocationPost,
		// do not write route call the commonRequestProcess in get
		"alltrade_delete_postgresxl_post":PostgresqlDelete,
	}

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
	metaResult := metadata.Interpret(metaInput,req.Payload)
	query := queryhelper.PrepareDeleteQuery(metaResult)

	var dbAbs model.DBAbstract
	dbAbs.DBType = "cassandra"
	dbAbs.QueryType = "INSERT"
	dbAbs.Query = query
	endpoint.Process(&dbAbs)

	return prepareResponse(dbAbs)
}

// StockAdjustmentGet handles StockAdjustment GET request

func StockAdjustmentGet(req model.RequestAbstract) model.ResponseAbstract {

	dbAbs := commonRequestProcess(req, "stock_adjustment")

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

        metaDataSelect = utils.DecodeJSON( returnString() )

	metaInput := utils.FindMap("table", table_name, metaDataSelect)
	metaResult := metadata.InterpretSelect(metaInput, req.Filters)

	// pagination and limit check
	metaResult["limit"] = req.Limit
	metaResult["token"] = req.Token

	var query []string
	var dbAbs model.DBAbstract
	dbAbs.QueryType = "SELECT"

	// For cassandra if we have stratio we want to make it cassandra_stratio
	if metaResult["databaseType"] == "cassandra" {

		if ! cassandra_helper.IsValidCassandraQuery(metaResult) {

			metaResult["databaseType"] = "cassandra_stratio"
		}

	}

	query = queryhelper.PrepareSelectQuery(metaResult)

	dbAbs.DBType = metaResult["databaseType"].(string)
	dbAbs.Query = query
	endpoint.Process(&dbAbs)

	return dbAbs
}

// ObtainDetailGet handles ObtainDetail GET request

func ObtainDetailGet(req model.RequestAbstract) model.ResponseAbstract {

	dbAbs := commonRequestProcess(req, "obtain_detail")

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

	dbAbs := commonRequestProcess(req, "sub_stock_detail_transfer")

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

	dbAbs := commonRequestProcess(req, "sub_stock_daily_detail")

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

	dbAbs := commonRequestProcess(req, "tranfer_out_mismatch")

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

	dbAbs := commonRequestProcess(req, "request_goods")

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

	dbAbs := commonRequestProcess(req, "order_transfer")

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

	dbAbs := commonRequestProcess(req, "sale_out_detail")

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

//	metaDataSelect = utils.DecodeJSON(utils.ReadFile(constant.HectorConf + "/metadata/alltrade/alltradeApi.json"))
        metaDataSelect = utils.DecodeJSON( jsonFileContentsApi )
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

	valid := true

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

			} else {
				valid = false
			}

		}

	}

	if !valid {
		query = ""
	}

	query = strings.Trim(query, " AND")
	var dbAbs model.DBAbstract
	dbAbs.DBType = "presto"
	dbAbs.QueryType = "SELECT"
	dbAbs.Query = []string{query}
	endpoint.Process(&dbAbs)

	return prepareResponse(dbAbs)
}

func PostgresqlGet(req model.RequestAbstract) model.ResponseAbstract  {
	metaDataOut:=utils.FindMap("table","userinfo",metaDataSelect)
	metaDataResult := metadata.InterpretSelect(metaDataOut,req.Filters)
	query  :=queryhelper.PrepareSelectQuery(metaDataResult)
	var dbAbs model.DBAbstract
	dbAbs.DBType = "postgresxl"
	dbAbs.QueryType = "SELECT"
	dbAbs.Query =query
	endpoint.Process(&dbAbs)
	return prepareResponse(dbAbs)
}
func UserLocationGet(req model.RequestAbstract) model.ResponseAbstract{
	metaDataOut:=utils.FindMap("table","user_location",metaDataSelect)
	metaDataResult := metadata.InterpretSelect(metaDataOut,req.Filters)
	query  :=queryhelper.PrepareSelectQuery(metaDataResult)
	var dbAbs model.DBAbstract
	dbAbs.DBType = "postgresxl"
	dbAbs.QueryType = "SELECT"
	dbAbs.Query =query
	endpoint.Process(&dbAbs)
	return prepareResponse(dbAbs)

}

func UserLocationPost(req model.RequestAbstract) model.ResponseAbstract {

	metaInput := utils.FindMap("table","user_location",metaData)
	metaResult := metadata.Interpret(metaInput,req.Payload)
	query:=queryhelper.PrepareInsertQuery(metaResult)
	var dbAbs model.DBAbstract
	dbAbs.DBType = "postgresxl"
	dbAbs.QueryType = "INSERT"
	dbAbs.Query = query
	endpoint.Process(&dbAbs)
	return prepareResponse(dbAbs)
}

/*  // Implemented as a common function..
func UserGroupLocationGet(req model.RequestAbstract) model.ResponseAbstract  {
	metaDataOut:=utils.FindMap("table","user_group_location",metaDataSelect)
	metaDataResult := metadata.InterpretSelect(metaDataOut,req.Filters)
	query  :=queryhelper.PrepareSelectQuery(metaDataResult)
	var dbAbs model.DBAbstract
	dbAbs.DBType = "postgresxl"
	dbAbs.QueryType = "SELECT"
	dbAbs.Query =query
	endpoint.Process(&dbAbs)
	return prepareResponse(dbAbs)
} */

func UserGroupLocationPost(req model.RequestAbstract) model.ResponseAbstract  {
	metaInput := utils.FindMap("table","user_group_location",metaData)
	metaResult := metadata.Interpret(metaInput,req.Payload)
	query:=queryhelper.PrepareInsertQuery(metaResult)
	var dbAbs model.DBAbstract
	dbAbs.DBType = "postgresxl"
	dbAbs.QueryType = "INSERT"
	dbAbs.Query = query
	endpoint.Process(&dbAbs)
	return prepareResponse(dbAbs)

}
/*  // Implemented as a common function..
func UserComponentGet(req model.RequestAbstract) model.ResponseAbstract{
	metaDataOut:=utils.FindMap("table","user_component",metaDataSelect)
	metaDataResult := metadata.InterpretSelect(metaDataOut,req.Filters)
	query  :=queryhelper.PrepareSelectQuery(metaDataResult)
	var dbAbs model.DBAbstract
	dbAbs.DBType = "postgresxl"
	dbAbs.QueryType = "SELECT"
	dbAbs.Query =query
	endpoint.Process(&dbAbs)
	return prepareResponse(dbAbs)
}
*/

func UserComponentPost(req model.RequestAbstract) model.ResponseAbstract  {
	metaInput := utils.FindMap("table","user_component",metaData)
	metaResult := metadata.Interpret(metaInput,req.Payload)
	query:=queryhelper.PrepareInsertQuery(metaResult)
	var dbAbs model.DBAbstract
	dbAbs.DBType = "postgresxl"
	dbAbs.QueryType = "INSERT"
	dbAbs.Query = query
	endpoint.Process(&dbAbs)
	return prepareResponse(dbAbs)
}

/*
func ProductMasterGet(req model.RequestAbstract) model.ResponseAbstract {

	// Table name gets passed
	// Request gets passed
	// Query is processed in dbAbstract
	// dbAbstract returned


	metaDataSelectFromConst := utils.DecodeJSON( returnString() )
	metaDataOutVar := utils.FindMap("table","tmp_product_master", metaDataSelectFromConst)

	fmt.Println("MetadataSelectFromConst : ", metaDataSelectFromConst)

	fmt.Println("Length of the base file is : ", len(returnString()))
	fmt.Println("Actual File : ", returnString())


	fmt.Println("Metadata Out is : ", metaDataOutVar)
	metaDataResult := metadata.InterpretSelect(metaDataOutVar, req.Filters)

	fmt.Println("Metadata Select is : ", metaDataResult)

	query  := queryhelper.PrepareSelectQuery(metaDataResult)

	var dbAbs model.DBAbstract
	dbAbs.DBType = "postgresxl"
	dbAbs.QueryType = "SELECT"
	dbAbs.Query = query
	endpoint.Process(&dbAbs)

//	fmt.Println(dbAbs)

	return prepareResponse(dbAbs)
}
*/


func ProductMasterGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_product_master"); return prepareResponse(dbAbs); }
func LocationShipToGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_location_ship_to"); return prepareResponse(dbAbs); }
func RouteMasterGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_route_master"); return prepareResponse(dbAbs); }
func LocationMappingPlantGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_location_mapping_plant"); return prepareResponse(dbAbs); }
func PrintHistoryGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_print_history"); return prepareResponse(dbAbs); }
func CompanyMasterGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_company"); return prepareResponse(dbAbs); }
func LocationGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_location"); return prepareResponse(dbAbs); }
func LocationSubStockGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_location_sub_stock"); return prepareResponse(dbAbs); }
func VendorMasterGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_vendor_master"); return prepareResponse(dbAbs); }
func SupplierMasterGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_supplier_master"); return prepareResponse(dbAbs); }
func ConfigRouteGroupGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_config_route_group"); return prepareResponse(dbAbs); }
func MasterDataGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_master_data"); return prepareResponse(dbAbs); }
func DeviceSpecificationGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_device_specification"); return prepareResponse(dbAbs); }
func LanguageGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_language"); return prepareResponse(dbAbs); }
func RunningFormatGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_running_format"); return prepareResponse(dbAbs); }
func LastRunningGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_last_running"); return prepareResponse(dbAbs); }
func MapLocationRouteGroupGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_map_location_route_group"); return prepareResponse(dbAbs); }
func ScheduleReportGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_schedule_report"); return prepareResponse(dbAbs); }
func LocationRelationalGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_location_relational"); return prepareResponse(dbAbs); }
func NewsFeedGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_news_feed"); return prepareResponse(dbAbs); }
func TodoListGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_todo_list"); return prepareResponse(dbAbs); }
func MenuGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_menu"); return prepareResponse(dbAbs); }
func AscMasterGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_asc_master"); return prepareResponse(dbAbs); }
func QtyPerTimeIntervalGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_qty_per_time_interval"); return prepareResponse(dbAbs); }
func UserLoginGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_user_login"); return prepareResponse(dbAbs); }
func UserAuthenGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_user_authen"); return prepareResponse(dbAbs); }
func UserGroupGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_user_group"); return prepareResponse(dbAbs); }
func UserComponentGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_user_component"); return prepareResponse(dbAbs); }
func UserGroupLocationGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_user_group_location"); return prepareResponse(dbAbs); }
func GroupLocationGet(req model.RequestAbstract) model.ResponseAbstract { dbAbs := commonRequestProcess(req, "tmp_group_location"); return prepareResponse(dbAbs); }


func PostgresqlUpdate(req model.RequestAbstract) model.ResponseAbstract{
	metaInput := utils.FindMap("table", "userinfo", metaData)
	metaResult := metadata.Interpret(metaInput, req.Payload)
	// data coming from postgres.json do not hardcode req.Payload
	query:=queryhelper.PrepareUpdateQuery(metaResult)

	var dbAbs model.DBAbstract
	dbAbs.DBType = "postgresxl"
	dbAbs.QueryType = "UPDATE"
	dbAbs.Query = query
	endpoint.Process(&dbAbs)

	return prepareResponse(dbAbs)
}
func PostgresqlDelete(req model.RequestAbstract) model.ResponseAbstract{
	metaInput := utils.FindMap("table", "userinfo", metaData)
	metaResult := metadata.Interpret(metaInput, req.Payload)
	query:=queryhelper.PrepareDeleteQuery(metaResult)
	var dbAbs model.DBAbstract
	dbAbs.DBType = "postgresxl"
	dbAbs.QueryType = "DELETE"
	dbAbs.Query = query
	endpoint.Process(&dbAbs)

	return prepareResponse(dbAbs)
}

func ReportsAdjustStockGet(req model.RequestAbstract) model.ResponseAbstract {
	valid := true
	query := `
	SELECT 	adjust_date_time,
			adjust_stock_no,
			brand_key,           
			commercial_name_key,
			company,
			location_code,
			location_name,
			mat_code_key,
			model_key,
			product_subtype_key, 
			product_type_key,    
			remark,              
			serial_key,
	FROM stock_adjustment
	`

	if len(req.Filters) > 0 {
		query += " WHERE "

		for k, v := range req.Filters {
			if k == "adjustStatus" {
				query += (" adjust_status = '" + v + "' AND")
			} else if k == "createDateTimeRange" {
				values := strings.Split(v, ",")
				if len(values) == 2 {

					query += (" create_datetime BETWEEN timestamp '" + values[0] + "' AND timestamp '" + values[1] + "'")
					query += " AND"
				}
			} else if k == "locationCode" {
				query += (" location_code IN (")

				values := strings.Split(v, ",")

				for _, val := range values {

					query += "'" + val + "' ,"
				}
				query = strings.Trim(query, ",")
				query += ") AND"
			} else if k == "locationSubType" {
				query += (" location_subtype = '" + v + "' AND")
			} else if k == "locationType" {
				query += (" location_type = '" + v + "' AND")
			} else if k == "matCode_key" {
				query += (" mat_code_key = '" + v + "' AND")
			} else if k == "serial_key" {
				query += (" serial_key = '" + v + "' AND")
			} else {
				valid = false
			}
		}
	}

	if !valid {
		query = ""
	}

	query = strings.Trim(query, " AND")
	var dbAbs model.DBAbstract
	dbAbs.DBType = "presto"
	dbAbs.QueryType = "SELECT"
	dbAbs.Query = []string{query}
	endpoint.Process(&dbAbs)

	return prepareResponse(dbAbs)
}

func ReportsCheckStockDetailGet(req model.RequestAbstract) model.ResponseAbstract {
	valid := true
	query := `
	SELECT 	brand_key,              
			commercial_name_key,
			confirm_datetime,       
			confirm_no,             
			confirm_user,
			location_code,          
			location_name,          
			mat_code_key,           
			model_key,              
			product_subtype_key,    
			product_type_key,
			stock_on_hand_qty
	FROM check_stock_detail		
	`

	if len(req.Filters) > 0 {
		query += " WHERE "

		for k, v := range req.Filters {
			if k == "createDateTimeRange" {
				values := strings.Split(v, ",")
				if len(values) == 2 {

					query += (" create_datetime BETWEEN timestamp '" + values[0] + "' AND timestamp '" + values[1] + "'")
					query += " AND"
				}
			} else if k == "locationCode" {
				query += (" location_code IN (")

				values := strings.Split(v, ",")

				for _, val := range values {

					query += "'" + val + "' ,"
				}
				query = strings.Trim(query, ",")
				query += ") AND"
			} else if k == "transactionType" {
				query += (" transaction_type = '" + v + "' AND")
			} else {
				valid = false
			}
		}
	}

	if !valid {
		query = ""
	}

	query = strings.Trim(query, " AND")
	var dbAbs model.DBAbstract
	dbAbs.DBType = "presto"
	dbAbs.QueryType = "SELECT"
	dbAbs.Query = []string{query}
	endpoint.Process(&dbAbs)

	return prepareResponse(dbAbs)

}

func ReportsDirectSaleDetailGet(req model.RequestAbstract) model.ResponseAbstract {
	valid := true
	query := `
	SELECT 	commercial_name_key,
			company,             
			create_by,           
			from_substock,       
			location_code,       
			location_name,       
			matcode_key,         
			sales_code,          
			serial_key,          
			sold_status         
	FROM sale_out_detail
	`
	if len(req.Filters) > 0 {
		query += " WHERE "

		for k, v := range req.Filters {
			if k == "createBy" {
				query += (" create_by = '" + v + "' AND")
			} else if k == "locationCode" {
				query += (" location_code IN (")

				values := strings.Split(v, ",")

				for _, val := range values {

					query += "'" + val + "' ,"
				}
				query = strings.Trim(query, ",")
				query += ") AND"
			} else if k == "locationRegion" {
				query += (" location_region = '" + v + "' AND")
			} else if k == "locationSubType" {
				query += (" location_subtype = '" + v + "' AND")
			} else if k == "locationType" {
				query += (" location_type = '" + v + "' AND")
				// TODO: Sales Code BETWEEN Query  - will do after the final queries come
			} else if k == "salesCode" {
				query += (" sales_code = '" + v + "' AND")
			} else if k == "soldStatus" {
				query += (" sold_status = '" + v + "' AND")
			} else if k == "transactionType" {
				query += (" transaction_type = '" + v + "' AND")
			} else {
				valid = false
			}
		}
	}

	if !valid {
		query = ""
	}

	query = strings.Trim(query, " AND")
	var dbAbs model.DBAbstract
	dbAbs.DBType = "presto"
	dbAbs.QueryType = "SELECT"
	dbAbs.Query = []string{query}
	endpoint.Process(&dbAbs)

	return prepareResponse(dbAbs)

}

func ReportsDirectSaleSummaryGet(req model.RequestAbstract) model.ResponseAbstract {
	valid := true
	query := `
	SELECT  brand_key,           
			commercial_name_key, 
			company,             
			location_code,       
			location_name,       
			matcode_key,         
			model_key,           
			product_subtype_key, 
			product_type_key,    
			sales_code   
	FROM sale_out_detail
	`
	if len(req.Filters) > 0 {
		query += " WHERE "

		for k, v := range req.Filters {
			if k == "createDateTimeRange" {
				values := strings.Split(v, ",")
				if len(values) == 2 {

					query += (" create_datetime BETWEEN timestamp '" + values[0] + "' AND timestamp '" + values[1] + "'")
					query += " AND"
				}
			} else if k == "locationCode" {
				query += (" location_code IN (")

				values := strings.Split(v, ",")

				for _, val := range values {

					query += "'" + val + "' ,"
				}
				query = strings.Trim(query, ",")
				query += ") AND"
			} else if k == "locationSubType" {
				query += (" location_subtype = '" + v + "' AND")
			} else if k == "locationType" {
				query += (" location_type = '" + v + "' AND")
				// TODO: Sales Code BETWEEN Query  - will do after the final queries come
			} else if k == "salesCode" {
				query += (" sales_code = '" + v + "' AND")
			} else if k == "soldStatus" {
				query += (" sold_status = '" + v + "' AND")
			} else if k == "transactionType" {
				query += (" transaction_type = '" + v + "' AND")
			} else {
				valid = false
			}
		}
	}

	if !valid {
		query = ""
	}

	query = strings.Trim(query, "AND")
	var dbAbs model.DBAbstract
	dbAbs.DBType = "presto"
	dbAbs.QueryType = "SELECT"

	dbAbs.Query = []string{query}
	endpoint.Process(&dbAbs)

	return prepareResponse(dbAbs)
}

func ReportsRequestGoodsSummaryGet(req model.RequestAbstract) model.ResponseAbstract {
	valid := true
	query := `
	SELECT 	company,
			create_by,
			create_datetime,
			from_location_code,
			mobile_no,
			picking_datetime,
			remark,
			request_detail,
			request_no,
			request_status,
			to_location_code,
			to_location_name
	FROM request_goods	
	`
	if len(req.Filters) > 0 {
		query += " WHERE "

		for k, v := range req.Filters {
			if k == "transactionType" {
				query += (" transaction_type = '" + v + "' AND")
			} else if k == "requestStatus" {
				query += (" request_status = '" + v + "' AND")
			} else if k == "fromLocationType" {
				query += (" from_location_type = '" + v + "' AND")
			} else if k == "fromLocationSubType" {
				query += (" from_location_subtype = '" + v + "' AND")
			} else if k == "company" {
				query += (" company = '" + v + "' AND")
			} else if k == "createDateTimeRange" {
				values := strings.Split(v, ",")
				if len(values) == 2 {

					query += (" create_datetime BETWEEN timestamp '" + values[0] + "' AND timestamp '" + values[1] + "'")
					query += " AND"
				}
			} else if k == "fromLocationCode" {

				query += (" from_location_code IN (")

				values := strings.Split(v, ",")

				for _, val := range values {

					query += "'" + val + "' ,"
				}
				query = strings.Trim(query, ",")
				query += ") AND"

			} else {
				valid = false
			}
		}
	}

	if !valid {
		query = ""
	}

	query = strings.Trim(query, " AND")
	var dbAbs model.DBAbstract
	dbAbs.DBType = "presto"
	dbAbs.QueryType = "SELECT"
	dbAbs.Query = []string{query}
	endpoint.Process(&dbAbs)

	return prepareResponse(dbAbs)
}

func ReportsTransferDetailGet(req model.RequestAbstract) model.ResponseAbstract {
	valid := true
	query := `
	SELECT 	request_goods.create_datetime,
			request_goods.request_no,
			request_goods.for_substock,
			request_goods.create_datetime              
	FROM request_goods JOIN sub_stock_detail_transfer
	ON request_goods.company = sub_stock_detail_transfer.company
	`
	if len(req.Filters) > 0 {
		query += " WHERE "

		for k, v := range req.Filters {
			if k == "transactionType" {
				query += (" sub_stock_detail_transfer.transaction_type = '" + v + "' AND")
			} else if k == "createDateTimeRange" {
				values := strings.Split(v, ",")
				if len(values) == 2 {

					query += (" create_datetime BETWEEN timestamp '" + values[0] + "' AND timestamp '" + values[1] + "'")
					query += " AND"
				}
			} else if k == "locationCode" {
				query += (" location_code IN (")

				values := strings.Split(v, ",")

				for _, val := range values {

					query += "'" + val + "' ,"
				}
				query = strings.Trim(query, ",")
				query += ") AND"
			} else if k == "locationSubType" {
				query += (" sub_stock_detail_transfer.location_subtype = '" + v + "' AND")
			} else if k == "locationType" {
				query += (" sub_stock_detail_transfer.location_type = '" + v + "' AND")
			} else {
				valid = false
			}
		}
	}

	if !valid {
		query = ""
	}

	query = strings.Trim(query, " AND")
	var dbAbs model.DBAbstract
	dbAbs.DBType = "presto"
	dbAbs.QueryType = "SELECT"
	dbAbs.Query = []string{query}
	endpoint.Process(&dbAbs)

	return prepareResponse(dbAbs)
}

func ReportsTransferSummaryGet(req model.RequestAbstract) model.ResponseAbstract {
	valid := true
	query := `
	SELECT 	request_goods.create_datetime,
			request_goods.request_no,
			request_goods.for_substock,
			request_goods.ship_to_code,
			request_goods.ship_to_name,        
			request_goods.mat_code_key,
			request_goods.do_no,
			request_goods.picking_datetime
	FROM request_goods JOIN sub_stock_detail_transfer
	ON request_goods.company = sub_stock_detail_transfer.company	
	`
	if len(req.Filters) > 0 {
		query += " WHERE "

		for k, v := range req.Filters {
			if k == "transactionType" {
				query += (" sub_stock_detail_transfer.transaction_type = '" + v + "' AND")
			} else if k == "createDateTimeRange" {
				values := strings.Split(v, ",")
				if len(values) == 2 {

					query += (" create_datetime BETWEEN timestamp '" + values[0] + "' AND timestamp '" + values[1] + "'")
					query += " AND"
				}
			} else if k == "locationCode" {
				query += (" location_code IN (")

				values := strings.Split(v, ",")

				for _, val := range values {

					query += "'" + val + "' ,"
				}
				query = strings.Trim(query, ",")
				query += ") AND"
			} else if k == "locationSubType" {
				query += (" sub_stock_detail_transfer.location_subtype = '" + v + "' AND")
			} else if k == "locationType" {
				query += (" sub_stock_detail_transfer.location_type = '" + v + "' AND")
			} else {
				valid = false
			}
		}
	}

	if !valid {
		query = ""
	}

	query = strings.Trim(query, " AND")
	var dbAbs model.DBAbstract
	dbAbs.DBType = "presto"
	dbAbs.QueryType = "SELECT"
	dbAbs.Query = []string{query}
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
