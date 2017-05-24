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
)

func ReturnRoutes() map[string]func(model.RequestAbstract) model.ResponseAbstract {

	// If ther eare custom functions, then this will be called.
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

		// Loop through the array
		for _, v := range dbAbs.RichData {

			var curRecord = make(map[string]interface{})

			// Map the values to curRecord
			mapRecord(v, &curRecord)

			// Make the time as per the format that they want..
			manipulateData(*dbAbs, &curRecord)



			retData = append(retData, curRecord)
		}


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

	if req.HTTPRequestType == "GET" {


		metaResult := metadata.InterpretSelect(table_name, req.Filters)

		// pagination and limit check
		metaResult["limit"] = req.Limit
		metaResult["token"] = req.Token
		metaResult["isOrCondition"] = req.IsOrCondition

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

		query = queryhelper.PrepareSelectQuery(metaResult)
		dbAbs.DBType = metaResult["databaseType"].(string)
		dbAbs.Query = query

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

			if isUpdateRequest {

				dbAbs.QueryType = "UPDATE"
				dbAbs.UpdateCondition = updateCondition
				metaResult["updateCondition"] = updateCondition

				query = queryhelper.PrepareUpdateQuery(metaResult)
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

		fmt.Println("Allow from filter", ok, " Filter fields :", metaResult)

		if _, ok := metaResult["updateCondition"].(map[string][]string); !ok {

			metaResult["updateCondition"] = map[string][]string{}
		}

		isUpdateRequest, updateKeyVals := getUpdateQueryConditions(metaInputPost, metaResult["updateCondition"].(map[string][]string))


		fmt.Println("Allow update query", isUpdateRequest, " Filter for where by query", updateKeyVals)

		// metaResult["updateCondition"] = updateKeyVals

		dbAbs.DBType = metaInputPost["databaseType"].(string)

		fmt.Println(metaResult)
		fmt.Println("Update condition", metaResult["updateCondition"])

//		if metaResult["put_supported"] == true {

			dbAbs.QueryType = "UPDATE"
			query := queryhelper.PrepareUpdateQuery( metaResult )
			logger.Write("INFO", string(query[0]))

		dbAbs.Query = query
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

	if _, ok := metaInputPost["database"].(string); !ok { return false, map[string][]string{}}
	if _, ok := metaInputPost["databaseType"]; !ok { return false, map[string][]string{}}
	if _, ok := metaInputPost["table"].(string); !ok { return false, map[string][]string{}}

	dbType := metaInputPost["databaseType"].(string)
	table_name := metaInputPost["table"].(string);
	primaryKey := ""

	if dbType == "cassandra" {

		dbName := metaInputPost["database"].(string)
		primaryKey = dbName + "." + table_name + "_pk"
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

		case "uuid" :
			updateKeyStr = dbAbs.RichData[0]["key"].(gocql.UUID).String()
		}

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