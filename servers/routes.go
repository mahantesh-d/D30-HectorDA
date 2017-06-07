package servers

import (
	"errors"
	"github.com/dminGod/D30-HectorDA/application/alltrade"
	"github.com/dminGod/D30-HectorDA/constant"
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/model"
	"strings"
	"encoding/json"
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/utils"
	"fmt"
)

// Routes store the mapping of routes to the underlying application logic
var Routes map[string]func(model.RequestAbstract) model.ResponseAbstract
var routesArr []map[string]func(model.RequestAbstract) model.ResponseAbstract

func init() {

	// Take routes from multiple applications
	allTradeRoutes := alltrade.ReturnRoutes()

	// This array currently only has one routes map defined, add more here for adding more applications
	routesArr = []map[string]func(model.RequestAbstract) model.ResponseAbstract{allTradeRoutes}

	// Make the Routes map otherwise go will throw an exception
	Routes = make(map[string]func(model.RequestAbstract) model.ResponseAbstract)

	// Loop over all the Apps
	for _, curAppMap := range routesArr {

		// Loop over all the string : methods and add each of them to the Routes map
		for key, value := range curAppMap {

			Routes[key] = value
		}
	}
}

// HandleRoutes is used resolve incoming routes and execute the corresponding application logic
func HandleRoutes(reqAbs model.RequestAbstract) (model.ResponseAbstract, error) {

	route := GetRouteName(reqAbs)

	reqAbs.RouteName = route

	reqAbs.AreDatesValid = true

	var respAbs model.ResponseAbstract


	// Get details from JSON
	routeDetails := utils.FindMap("apiName", route, config.Metadata_get());
	doesRouteExist := len(routeDetails) > 0 || RouteExists(route)

	// Route does not exist in the hardcode and in the JSON mapping.. exit.
	if ! doesRouteExist {

		logger.Write("ERROR", "Route for Application: "+reqAbs.Application+", Action: "+reqAbs.Action+", RequestType: "+reqAbs.HTTPRequestType+" not found")
		return model.ResponseAbstract{

			StatusCode : 501,
			Status : "fail",
			StandardStatusMessage : "NOT_FOUND",
			Text : "The given route was not found",
			Data : "{}",
			Count : 0,
		}, errors.New("Route not found")
	}

	// We know route does exist, lets enrich
	// All the hooks for global level and applciation will be applied here for changing the requests
	enrichRequest(&reqAbs)

	 if reqAbs.AreDatesValid == false {

		 return model.ResponseAbstract{

			 StatusCode : 501,
			 Status : "fail",
			 StandardStatusMessage : "NOT_FOUND",
			 Text : reqAbs.DateErrors[0],
			 Data : "{}",
			 Count : 0,
		 }, errors.New("Route not found")
	 }

	if RouteExists(route) {

		reqAbs.ApiName = route

		// Calling the custom method defined
		respAbs = Routes[route](reqAbs)

	} else {

		logger.Write("INFO", "Route not found in explict hardcode, checking for table entries in the JSON file")

		routeDetails := utils.FindMap("apiName", route, config.Metadata_get());

		if len(routeDetails) != 0 {

			logger.Write("INFO", "Calling common method for the request")

			mapRequestFields(&reqAbs, routeDetails)

			fmt.Println(reqAbs)

			respAbs = alltrade.HandleUnlistedRequest(reqAbs, routeDetails["table"].(string))
		}
	}

	// After getting a response, add the originating request with the response as well so you can use it
	// in other places
	respAbs.RequestAbs = reqAbs

	// All the hooks for global level and applciation will be applied here for changing the response
	enrichResponse(&respAbs)

	return respAbs, nil
}


func mapRequestFields(reqqAbs *model.RequestAbstract, routeDetails map[string]interface{}) {

	(*reqqAbs).DatabaseType = utils.ReturnMapStringVal(routeDetails, "databaseType")


	(*reqqAbs).DatabaseName = utils.ReturnMapStringVal(routeDetails, "database")

	(*reqqAbs).Table = utils.ReturnMapStringVal(routeDetails, "table")
	(*reqqAbs).ApiName = utils.ReturnMapStringVal(routeDetails, "apiName")
	(*reqqAbs).IsPutSupported = utils.ReturnMapBoolVal(routeDetails, "put_supported")

	if _, ok := routeDetails["fields"].(map[string]interface{}); ok {

		for _, curField := range routeDetails["fields"].(map[string]interface{}) {

			f := model.Field{}
			allFields := curField.(map[string]interface{})

			f.FieldName = utils.ReturnMapStringVal(allFields, "name")
			f.ColumnType =  utils.ReturnMapStringVal(allFields, "type")
			f.ColumnName =  utils.ReturnMapStringVal(allFields, "column")
			f.IsMultiValue =  utils.ReturnMapBoolVal(allFields, "valueType")
			f.Tags =  utils.ReturnMapSliceStringVal(allFields, "tags")
			f.IsGetField =  utils.ReturnMapBoolVal(allFields, "is_get_field")
			f.IsPutField =  utils.ReturnMapBoolVal(allFields, "is_put_field")
			f.IsPutFilterField =  utils.ReturnMapBoolVal(allFields, "is_put_filter_field")

			//(*reqqAbs).TableFields = append(reqqAbs.TableFields, f)
		}
	}
}


// RouteExists is used to check if a given route exists
// For example:
//  RoutesExists("alltrade_stock_adjustment_post")
// Output:
//  true
func RouteExists(route string) bool {

	// iterate over each route
	for k := range Routes {

		if route == k {
			return true
		}
	}

	return false
}

// Here are hooks for each request coming in so we can modify / enrich all requests coming in.
// The hooks are given on 2 levels -- Application level, which is each request hitting the application will go
// through this enrichAllRequest method and second level is the application specific enrich method.
// here any logic that needs to be implemented on the application specific level but you dont want to do an each
// API level can be done here.

func enrichRequest(reqAbs *model.RequestAbstract) {

	reqAbs.AdditionalData = make(map[string]interface{})

	// 2 Level filters
	// Hector level where all the requests will pass through
	enrichAllRequests(reqAbs)

	reqObjBytes, _ := json.Marshal(reqAbs)

	logger.Write("INFO", "Got Request " + string(reqObjBytes) )

	// This particular application level
	// For now we will just do a simple if conditional, later we want to move this out

	// TODO: Make a global, application to package level mapping -- each application package will have an implementation of the global filter in it
	if reqAbs.Application == "alltrade" {

		alltrade.EnrichRequest(reqAbs)
	}

}

// This is similar to the hooks for requests  -- for each request coming in so we can modify / enrich all responses going out.
// The hooks are given on 2 levels -- Application level, which is each request hitting the application will go
// through this enrichAllRequest method and second level is the application specific enrich method.
// here any logic that needs to be implemented on the application specific level but you dont want to do an each
// API level can be done here.

func enrichResponse(respAbs *model.ResponseAbstract) {

	enrichAllResponses(respAbs)

	resObjBytes, _ := json.Marshal(respAbs)

	logger.Write("INFO", "Gave Response " + string(resObjBytes) )

	if respAbs.RequestAbs.Application == "alltrade" {

		// alltrade.EnrichResponse(respAbs)
	}
}

// GetRouteName is used to return the route mapping as per the naming convention of Hector
func GetRouteName(reqAbs model.RequestAbstract) string {

	// route := strings.ToLower(reqAbs.Application + constant.HectorRouteDelimiter + reqAbs.Action + constant.HectorRouteDelimiter + reqAbs.HTTPRequestType)

	route := strings.ToLower( reqAbs.Application + constant.HectorRouteDelimiter + reqAbs.Action )

	return route
}
