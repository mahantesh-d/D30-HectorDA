package alltrade

import (
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/endpoint"
	"github.com/dminGod/D30-HectorDA/endpoint/cassandra_helper"
	"github.com/dminGod/D30-HectorDA/lib/queryhelper"
	"github.com/dminGod/D30-HectorDA/logger"
	_ "github.com/dminGod/D30-HectorDA/logger" // TODO: Add Intermdiate logs to track detailed activity of each API
	"github.com/dminGod/D30-HectorDA/metadata"
	"github.com/dminGod/D30-HectorDA/model"
	"github.com/dminGod/D30-HectorDA/utils"
	"strings"
	"github.com/gocql/gocql"
	"strconv"
	"reflect"
	"sync"
	"github.com/dminGod/D30-HectorDA/endpoint/postgresql_helper"
)

func ReturnRoutes() map[string]func(model.RequestAbstract) model.ResponseAbstract {

	// If there are custom functions, then this will be called.
	routes := map[string]func(model.RequestAbstract) model.ResponseAbstract{
	// Example
	//"alltrade_product_master_post" :  ProductMasterPost,
	}

	return routes
}

func EnrichRequest(reqAbs *model.RequestAbstract) { }

func EnrichResponse(reqAbs *model.ResponseAbstract) { }

func EnrichDataResponse(dbAbs *model.DBAbstract) {


	var retData []map[string]interface{}

	if len(dbAbs.RichData) > 0 {

		var wg sync.WaitGroup

		fieldsConfig := utils.GetLimitedTableDetails(dbAbs.TableName)

		// Loop through the array
		for _, v := range dbAbs.RichData {

			wg.Add(1)

			var curRecord = make(map[string]interface{})

			// Map the values to curRecord
			mapRecord(v, &curRecord)

			// Make the time as per the format that they want..
			go manipulateData(*dbAbs, curRecord, fieldsConfig, &retData, &wg )
			// retData = append(retData, curRecord)
		}

		wg.Wait()

	}

	dbAbs.Data = utils.EncodeJSON(retData)
}


func HandleUnlistedRequest(req model.RequestAbstract, table_name string) model.ResponseAbstract {

	var dbAbs model.DBAbstract

//	TestQuery()

//	cassandra_helper.TestSelectQuery()

	if req.HTTPRequestType == "GET" || req.HTTPRequestType == "POST" || req.HTTPRequestType == "PUT" ||  req.HTTPRequestType == "DELETE"{

		dbAbs = commonRequestProcess(req, table_name)
		EnrichDataResponse(&dbAbs)
	} else {

		return returnFailResponse("There was an error procesing your request", "REQUEST FAILED! Mapping data not found for route "+req.RouteName)
	}

	return prepareResponse(dbAbs)
}

func commonRequestProcess(req model.RequestAbstract, table_name string) model.DBAbstract {

	var dbAbs = model.DBAbstract{}

	dbAbs.TableName = table_name
	isOk := true

	isCassInsert := false
	var cassDbAbs model.DBAbstract


	if req.HTTPRequestType == "GET" {

		metaResult := metadata.InterpretSelect(table_name, req.Filters)

		// pagination and limit check
		metaResult["limit"] = strconv.Itoa(int(req.Limit))
		metaResult["offset"] = strconv.Itoa(int(req.Offset))

		metaResult["token"] = req.Token
		metaResult["isOrCondition"] = req.IsOrCondition
		metaResult["ComplexQuery"] = req.ComplexFilters
		metaResult["SelectFields"] = req.TableFields

		var query []string

		dbAbs.QueryType = "SELECT"
		dbAbs.IsOrCondition = req.IsOrCondition

		// For cassandra if we have stratio we want to make it cassandra_stratio
		if metaResult["databaseType"] == "cassandra" {

			if !cassandra_helper.IsValidCassandraQuery(metaResult) || metaResult["isOrCondition"].(bool) {

				logger.Write("INFO", "Selecting cassandra_stratio as index")
				metaResult["databaseType"] = "cassandra_stratio"
			}
		}




		query, isOk = queryhelper.PrepareSelectQuery(metaResult, req)
		dbAbs.DBType = metaResult["databaseType"].(string)
		dbAbs.Query = query

		if !isOk {

			dbAbs.Message = "Error: There was an error in the condition passed in the query"
			dbAbs.Count = 0
			dbAbs.Status = "fail"

			return dbAbs
		}


	} else if req.HTTPRequestType == "POST" {


		metaInputPost := utils.FindMap("table", table_name, config.Metadata_insert())

		metaResult := metadata.Interpret(metaInputPost, req.Payload, req.ComplexFilters)

		dbAbs.DBType = metaInputPost["databaseType"].(string)


		if  metadata.AllAPIs.APIHasInsertMethod(table_name) == false {

			dbAbs.Message = "Error: This API does not support the insert method"
			dbAbs.Count = 0
			dbAbs.Status = "fail"

			return dbAbs
		}


		// Does it have tag Cassandra?
		if _, ok := metaInputPost["tags"].([]interface{}); ok && len(metaInputPost["tags"].([]interface{})) > 0 {

			isCassInsert = utils.MatchFieldTag(metaInputPost["tags"].([]interface{}), "D30_API")
		}

		var query []string

		isUpdateRequest := false
		var updateCondition map[string][]string

		// Only for POST coming in for Update, used for Cassandra
		if metaResult["possibleUpdateRequest"].(bool) {

			logger.Write("INFO", "Possible update request from commonRequestProcess.")

			// Could be possibly be an update request
			//getPK :=

			if _, ok := metaResult["updateCondition"].(map[string][]string); !ok {

				metaResult["updateCondition"] = map[string][]string{}
			}

			isUpdateRequest, updateCondition = getUpdateQueryConditions(metaInputPost, metaResult["updateCondition"].(map[string][]string))

			metaResult["is_post_update"] = true

			if isUpdateRequest {

				dbAbs.QueryType = "UPDATE"
				dbAbs.UpdateCondition = updateCondition
				metaResult["updateCondition"] = updateCondition

				query, isOk = queryhelper.PrepareUpdateQuery(metaResult)


				if !isOk {

					dbAbs.Message = "Error: There was an error in the condition passed in the parameters"
					dbAbs.Count = 0
					dbAbs.Status = "fail"

					return dbAbs
				}
			}
		}

		 if isUpdateRequest == false {

			// This is a regular POST request

			 dbAbs.QueryType = "INSERT"
			 query, isOk = queryhelper.PrepareInsertQuery( metaResult )


			 // API Supports Delete?
			 if metadata.AllAPIs.APIHasDeleteMethod(table_name) {

			 // Remove this later
			 logger.Write("INFO", "API DOES SUPPORT DELETE")

				 // Get the primary keys and values
				 // Generate a PK select query
				PrimaryKeysWithValue, isOk := metadata.AllAPIs.GetAllPrimaryKeysWithValue(table_name, req.Payload)

				recExist, recExistOk := doRecordsExistAlready(table_name, PrimaryKeysWithValue)


				 if recExistOk == false {

					 dbAbs.Message = "Error: There was an error processing your request"
					 dbAbs.Count = 0
					 dbAbs.Status = "fail"

					 return dbAbs
				 }

				 if recExist {

					 dbAbs.Message = "Error: Entry already exists, duplicate entry not permitted"
					 dbAbs.Count = 0
					 dbAbs.Status = "fail"

					 return dbAbs
				 }


				 logger.Write("INFO", "PrimaryKeysWithValue", PrimaryKeysWithValue, "Table name", table_name, "Request payload", req.Payload)

				 if isOk {

					 delExists, delOk := doDeletedRecordsExist(table_name, PrimaryKeysWithValue)

					 if delOk == false {

						 dbAbs.Message = "Error: There was an error processing your request"
						 dbAbs.Count = 0
						 dbAbs.Status = "fail"

						 return dbAbs
					 }


					 if delExists {

						logger.Write("INFO", "Records do exist for this db need to clean.")
						cleanQuery, isOk := postgresql_helper.CleanDeletedRecord(table_name, PrimaryKeysWithValue)

						if isOk {
							cassDbAbs = model.DBAbstract{
								 DBType: "postgresxl",
								 Query: cleanQuery,
								 QueryType: "DELETE",
								 TableName: table_name,
							}

							dbAbs.QueryType = "DELETE_INSERT"
							logger.Write("INFO", "Changed the method to DELETE_INSERT")
						}
					 } else {

						 logger.Write("INFO", "No records dont need to clean.")
					 }
				 }
			 } else {

				 // Remove this later
				 logger.Write("INFO", "------->>> API DOES NOT SUPPORT DELETE")

			 }


			 if !isOk {

				 dbAbs.Message = "Error: There was an error in the condition passed in the parameters"
				 dbAbs.Count = 0
				 dbAbs.Status = "fail"

				 return dbAbs
			 }

			 logger.Write("INFO", string(query[0]))

		}

		if isCassInsert {

			var cassQuery []string

			cassQuery, isOk = cassandra_helper.InsertQueryBuild(metaResult)

			if isOk {

				cassDbAbs = model.DBAbstract{
					DBType: "cassandra",
					Query: cassQuery,
					QueryType: "INSERT",
					TableName: table_name,
				}
			} else {

				dbAbs.Message = "Error: There was an error in the condition passed in the parameters"
				dbAbs.Count = 0
				dbAbs.Status = "fail"

				return dbAbs
			}

		}

		dbAbs.Query = query


	} else if req.HTTPRequestType == "PUT" {

		metaInputPost := utils.FindMap("table", table_name, config.Metadata_insert())

		// Get the filters from the request
		metaResult, ok := metadata.InterpretUpdateFilters(metaInputPost, req.Payload, req.Filters)

		metaResult["ComplexQuery"] = req.ComplexFilters
		metaResult["is_post_update"] = false

		// Remove this later
		logger.Write("INFO", "Allow from filter", ok, " Filter fields :", metaResult)

		if _, ok := metaResult["updateCondition"].(map[string][]string); !ok {

			metaResult["updateCondition"] = map[string][]string{}
		}

		//isUpdateRequest, updateKeyVals := getUpdateQueryConditions(metaInputPost, metaResult["updateCondition"].(map[string][]string))


		// metaResult["updateCondition"] = updateKeyVals

		dbAbs.DBType = metaInputPost["databaseType"].(string)

		if metaResult["put_supported"] == true {

//			if _, ok := metaResult["updateCondition"].(map[string][]string); ok && len(metaResult["updateCondition"].(map[string][]string)) > 0 {


				dbAbs.QueryType = "UPDATE"
				var query []string
				query, isOk = queryhelper.PrepareUpdateQuery( metaResult )

				if (len(query) > 0) {
				logger.Write("INFO", string(query[0]))

				dbAbs.Query = query
				} else {

					isOk = false
				}

				if !isOk {

					dbAbs.Message = "Error: There was an error in the condition passed in the query"
					dbAbs.Count = 0
					dbAbs.Status = "fail"

					return dbAbs
				}
		} else {

				dbAbs.Message = "Error: Put not supported on this API"
				dbAbs.Count = 0
				dbAbs.Status = "fail"

				return dbAbs
			}
//		}
	} else if req.HTTPRequestType == "DELETE" {

		metaInputPost := utils.FindMap("table", table_name, config.Metadata_insert())

		metaResult := make(map[string]interface{})

		// Get the details of the object
		metaResult["databaseType"] = metaInputPost["databaseType"]
		metaResult["version"] = metaInputPost["version"]
		metaResult["database"] = metaInputPost["database"]
		metaResult["table"] = metaInputPost["table"]

		metaResult["field_keyvalue"] = map[string]interface{}{ "int_is_deleted" : "Y" }
		metaResult["field_keymeta"] = map[string]interface{}{ "int_is_deleted" : "text" }

		metaResult["updateCondition"] = map[string][]string{}
		metaResult["fields"] = make(map[string]interface{})
		metaResult["ComplexQuery"] = req.ComplexFilters
		metaResult["is_post_update"] = false

		supportsDelete := metadata.AllAPIs.APIHasDeleteMethod(metaInputPost["table"].(string))

		dbAbs.DBType = metaInputPost["databaseType"].(string)

		if supportsDelete {

			dbAbs.QueryType = "UPDATE"
			var query []string
			query, isOk = queryhelper.PrepareUpdateQuery( metaResult )

			if (len(query) > 0) && (isOk == true) {

				logger.Write("INFO", string(query[0]))
				dbAbs.Query = query
			} else {

				isOk = false
			}

			if !isOk {

				dbAbs.Message = "Error: There was an error in the condition passed in the query"
				dbAbs.Count = 0
				dbAbs.Status = "fail"

				return dbAbs
			}
		} else {

			dbAbs.Message = "Error: Delete not supported on this API"
			dbAbs.Count = 0
			dbAbs.Status = "fail"

			return dbAbs
		}

	}


	logger.Write("INFO", "dbAbs object before processing query", dbAbs)

	rowsAffected := endpoint.Process(&dbAbs, &cassDbAbs, isCassInsert)

	if req.HTTPRequestType == "DELETE" && dbAbs.Status == "success" {

		if rowsAffected == 0 {

			dbAbs.Message = "No rows found to delete, rows affected " + strconv.Itoa(rowsAffected)
		} else {

			dbAbs.Message = "Delete successful, rows affected " + strconv.Itoa(rowsAffected)
		}
	}


	if req.HTTPRequestType == "PUT" && isCassInsert == false {

		if rowsAffected == 0 {

			dbAbs.Message = "No rows found to update, rows affected " + strconv.Itoa(rowsAffected)
		} else {

			dbAbs.Message = "Update successful, rows affected " + strconv.Itoa(rowsAffected)
		}
	}

	return dbAbs
}


func doRecordsExistAlready(table_name string, PrimaryKeysWithValue []metadata.Field) (bool, bool) {

	retBool := false

	query, isOk := postgresql_helper.CountRecordsExist(table_name, PrimaryKeysWithValue)

	logger.Write("INFO", "This is the query to check for doRecordsExistAlready ", query)

	if !isOk {

		return false, false
	}

	dbAbs := model.DBAbstract{
		Query: query ,
		DBType: "postgresxl",
		QueryType: "SELECT",
	}

	endpoint.Process(&dbAbs, &model.DBAbstract{}, false)

	logger.Write("INFO", "dbAbs after query process : ", dbAbs)

	if len(dbAbs.RichData) > 0 {

		if _, ok := dbAbs.RichData[0]["cnt"]; ok {

			if int(dbAbs.RichData[0]["cnt"].(int64)) > 0 {

				logger.Write("INFO", "Count fromt he database > 0 for INSERT", dbAbs.RichData[0]["cnt"].(int64))
				retBool = true
			} else {

				logger.Write("INFO", "Count fromt he database NOT > 0 for INSERT", dbAbs.RichData[0]["cnt"].(int64))
			}
		}
	}

	return retBool, true
}





func doDeletedRecordsExist(table_name string, PrimaryKeysWithValue []metadata.Field) (bool, bool) {

	retBool := false

	query, isOk := postgresql_helper.CountDeletedRecords(table_name, PrimaryKeysWithValue)

	logger.Write("INFO", "This is the query to check for doDeletedRecordsExist ", query)

	if !isOk {

		return false, false
	}

	dbAbs := model.DBAbstract{
		Query: query ,
		DBType: "postgresxl",
		QueryType: "SELECT",
	}

	endpoint.Process(&dbAbs, &model.DBAbstract{}, false)

	logger.Write("INFO", "dbAbs after query process : ", dbAbs)

	if len(dbAbs.RichData) > 0 {

		if _, ok := dbAbs.RichData[0]["cnt"]; ok {

			if int(dbAbs.RichData[0]["cnt"].(int64)) > 0 {

				logger.Write("INFO", "Count fromt he database > 0 for INSERT", dbAbs.RichData[0]["cnt"].(int64))
				retBool = true
			} else {

				logger.Write("INFO", "Count fromt he database not > 0 for INSERT", dbAbs.RichData[0]["cnt"].(int64))
			}
		}
	}

	return retBool, true
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


func handleCustomDateManipulation(queryStr *string, table_name string, column_type string, key string, value string) {

	if table_name == "check_stock_detail" && key == "confirm_datetime" {

		onlyDate := value[:10]
		*queryStr += " " + key + " > '" + onlyDate + " 00:00:00'  AND " + key + " < '" + onlyDate + " 23:59:59' AND"
	}
}







func getUpdateQueryConditions(metaInputPost map[string]interface{}, updateCondition map[string][]string) (bool, map[string][]string) {

	if _, ok := metaInputPost["database"].(string); !ok {

		logger.Write("ERROR", "Metainput database not set");
		return false, map[string][]string{} }


	if _, ok := metaInputPost["databaseType"]; !ok {

		logger.Write("ERROR", "Metainput databaseType not set");
		return false, map[string][]string{}}


	if _, ok := metaInputPost["table"].(string); !ok {

		logger.Write("ERROR", "Metainput table not set");
		return false, map[string][]string{}}


	dbType := metaInputPost["databaseType"].(string)
	table_name := metaInputPost["table"].(string);
	primaryKey := ""

	if len(updateCondition) == 0 {

		logger.Write("ERROR", "getUpdateQueryConditions for cassandra did not have updateCondition returning.")
		return false, map[string][]string{}
	}


	if dbType == "cassandra" {

		dbName := metaInputPost["database"].(string)
		primaryKey =  table_name + "_pk"
		table_name = dbName + "." + table_name

	} else {

		primaryKey = table_name + "_pk"
	}

	query := "SELECT " + primaryKey + " as key FROM " + table_name + " WHERE "

	for k, v := range updateCondition {

		colDetails := utils.GetColumnDetailsByColumnName(table_name, k)

		if colDetails["valueType"].(string) == "single" && colDetails["type"].(string) == "timestamp" {

			passValue := ""

			if len(v) > 0 {

				passValue = v[0]
			}

			handleCustomDateManipulation(&query, table_name, colDetails["type"].(string), k, passValue)

		} else if colDetails["valueType"].(string) == "single" {
			// 1. Handle Multi column query here
			// Range query for date as well
			for _, vv := range v {

				query += " " + k + " = '" + vv + "' AND"
			}
		} else if colDetails["valueType"].(string) == "multi" {

			if len(v) > 0 {

				query += " " + k + " = (ARRAY["

				for _, vv := range v {

					query += "'" + vv + "',"
				}

				query = strings.Trim(query, ",")

				query += "]) AND"
			}
		}
	}

	query = strings.Trim(query, "AND")

	query += " LIMIT 1"

	if dbType == "cassandra" {

		query += " ALLOW FILTERING"

	}


	dbAbs := model.DBAbstract{ Query: []string{ query },
		DBType: dbType,
		QueryType: "SELECT",
	}

	endpoint.Process( &dbAbs, &model.DBAbstract{}, false )

	logger.Write("INFO", "getUpdateQueryConditions result from the query dbAbs", dbAbs)

	var updateKeyStr string

	if len(dbAbs.RichData) > 0 {

		if _, ok := dbAbs.RichData[0]["key"]; ok {

		switch reflect.TypeOf( dbAbs.RichData[0]["key"] ).String() {

		case "int64" :
			updateKeyStr = strconv.Itoa(int( dbAbs.RichData[0]["key"].(int64) ))

		case "string" :
			updateKeyStr = dbAbs.RichData[0]["key"].(string)

		case "gocql.UUID" :
			updateKeyStr = dbAbs.RichData[0]["key"].(gocql.UUID).String()
		}

		logger.Write("INFO", "From update query Returning " + primaryKey + " : " + updateKeyStr)
		return true, map[string][]string{ primaryKey : []string{ updateKeyStr } }
		}
	} else {


	}

	return false, map[string][]string{}
}




func returnFailResponse(messageToUser string, messageToLog string) model.ResponseAbstract {

	logger.Write("ERROR", "AlltradeAPI Fail: " + messageToLog + " messageToUser : " + messageToUser)

	return model.ResponseAbstract{
		Status:                "500",
		StandardStatusMessage: "error",
		Text:  "There was an error" + messageToUser,
		Data:  "Error : " + messageToUser,
		Count: 0,
	}
}