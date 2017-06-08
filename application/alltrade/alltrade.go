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
	"fmt"
	"strings"
	"github.com/gocql/gocql"
	"strconv"
	"reflect"
	"sync"
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

		// Loop through the array
		for _, v := range dbAbs.RichData {

			wg.Add(1)

			var curRecord = make(map[string]interface{})

			// Map the values to curRecord
			mapRecord(v, &curRecord)

			// Make the time as per the format that they want..
			go manipulateData(*dbAbs, curRecord, &retData, &wg)
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

	if req.HTTPRequestType == "GET" || req.HTTPRequestType == "POST" || req.HTTPRequestType == "PUT" {

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




		query, isOk = queryhelper.PrepareSelectQuery(metaResult)
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

		metaResult := metadata.Interpret(metaInputPost, req.Payload)

		dbAbs.DBType = metaInputPost["databaseType"].(string)

		var query []string

		isUpdateRequest := false
		var updateCondition map[string][]string

		// Only for POST coming in for Update, used for Cassandra
		if metaResult["possibleUpdateRequest"].(bool) {

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

					dbAbs.Message = "Error: There was an error in the condition passed in the query"
					dbAbs.Count = 0
					dbAbs.Status = "fail"

					return dbAbs
				}
			}
		}

		 if isUpdateRequest == false {

			// This is a regular POST request

			dbAbs.QueryType = "INSERT"
			query = queryhelper.PrepareInsertQuery( metaResult )
			logger.Write("INFO", string(query[0]))

		}

		dbAbs.Query = query


	} else if req.HTTPRequestType == "PUT" {

		metaInputPost := utils.FindMap("table", table_name, config.Metadata_insert())

		// Get the filters from the request
		metaResult, ok := metadata.InterpretUpdateFilters(metaInputPost, req.Payload, req.Filters)

		metaResult["ComplexQuery"] = req.ComplexFilters
		metaResult["is_post_update"] = false

		fmt.Println("Allow from filter", ok, " Filter fields :", metaResult)

		if _, ok := metaResult["updateCondition"].(map[string][]string); !ok {

			metaResult["updateCondition"] = map[string][]string{}
		}

		//isUpdateRequest, updateKeyVals := getUpdateQueryConditions(metaInputPost, metaResult["updateCondition"].(map[string][]string))


		// metaResult["updateCondition"] = updateKeyVals

		dbAbs.DBType = metaInputPost["databaseType"].(string)

		fmt.Println(metaResult)


		if metaResult["put_supported"] == true {

//			if _, ok := metaResult["updateCondition"].(map[string][]string); ok && len(metaResult["updateCondition"].(map[string][]string)) > 0 {

//				fmt.Println("Update condition 198.", metaResult["updateCondition"])
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
	}


	fmt.Println("Running query ", dbAbs)
	endpoint.Process(&dbAbs)

	return dbAbs
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

		for _, vv := range v {

			query +=  " " + k + " = '" + vv + "' AND"
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

	endpoint.Process( &dbAbs )

	fmt.Println("Result from the query", dbAbs)

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


		fmt.Println(reflect.TypeOf( dbAbs.RichData[0]["key"] ).String())

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