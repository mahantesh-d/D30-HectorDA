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
)


func ReturnRoutes() map[string]func(model.RequestAbstract) model.ResponseAbstract {

	// If ther eare custom functions, then this will be called.
	routes := map[string]func(model.RequestAbstract) model.ResponseAbstract{
	// Example
	//"alltrade_product_master_post" :  ProductMasterPost,
	}

	return routes
}

func EnrichRequest(reqAbs *model.RequestAbstract) {

}

func EnrichResponse(reqAbs *model.ResponseAbstract) {

}

func HandleUnlistedRequest(req model.RequestAbstract, table_name string) model.ResponseAbstract {

	var dbAbs model.DBAbstract

	if req.HTTPRequestType == "GET" || req.HTTPRequestType == "POST" {

		dbAbs = commonRequestProcess(req, table_name)

	} else {

		return returnFailResponse("There was an error procesing your request", "REQUEST FAILED! Mapping data not found for route "+req.RouteName)
	}

	return prepareResponse(dbAbs)
}

func commonRequestProcess(req model.RequestAbstract, table_name string) model.DBAbstract {

	var dbAbs = model.DBAbstract{}

	if req.HTTPRequestType == "GET" {

		metaInput := utils.FindMap("table", table_name, config.Metadata_get)
		metaResult := metadata.InterpretSelect(metaInput, req.Filters)

		// pagination and limit check
		metaResult["limit"] = req.Limit
		metaResult["token"] = req.Token

		var query []string

		dbAbs.QueryType = "SELECT"

		// For cassandra if we have stratio we want to make it cassandra_stratio
		if metaResult["databaseType"] == "cassandra" {

			if !cassandra_helper.IsValidCassandraQuery(metaResult) {

				metaResult["databaseType"] = "cassandra_stratio"
			}

		}

		query = queryhelper.PrepareSelectQuery(metaResult)
		dbAbs.DBType = metaResult["databaseType"].(string)
		dbAbs.Query = query

	} else if req.HTTPRequestType == "POST" {

		metaInputPost := utils.FindMap("table", table_name, config.Metadata_insert)

		metaResult := metadata.Interpret(metaInputPost, req.Payload)
		query := queryhelper.PrepareInsertQuery(metaResult)

		logger.Write("INFO", query[0])

		dbAbs.DBType = metaInputPost["databaseType"].(string)
		dbAbs.QueryType = "INSERT"
		dbAbs.Query = query
	}

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

func returnFailResponse(messageToUser string, messageToLog string) model.ResponseAbstract {

	logger.Write("ERROR", "AlltradeAPI Fail: "+messageToLog+" messageToUser : "+messageToUser)

	return model.ResponseAbstract{

		Status:                "500",
		StandardStatusMessage: "error",
		Text:  "There was an error" + messageToUser,
		Data:  "Error : " + messageToUser,
		Count: 0,
	}
}