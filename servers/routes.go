package servers

import(
	"github.com/dminGod/D30-HectorDA/model"
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/application/alltrade"
	"github.com/dminGod/D30-HectorDA/constant"
	"errors"
	"strings"
)


var Routes map[string] func(model.RequestAbstract) model.ResponseAbstract

func init() {
	
	Routes = map[string] func(model.RequestAbstract) model.ResponseAbstract {
		"alltrade_foo_post" : alltrade.Foo_Post,
		"alltrade_foo_get" : alltrade.Foo_Get,
		"alltrade_stock_adjustment_post" : alltrade.StockAdjustment_Post,
		"alltrade_foobar_post" : alltrade.Foobar_Post}
}



func HandleRoutes(reqAbs model.RequestAbstract) (model.ResponseAbstract,error) {

	route := GetRouteName(reqAbs)

	// check if the route exists
	if !RouteExists(route) {
		logger.Write("ERROR", "Route for Application: " + reqAbs.Application + ", Action: " + reqAbs.Action + ", RequestType: " + reqAbs.HTTPRequestType + " not found")
		return model.ResponseAbstract{}, errors.New("Route not found")
	}

	return Routes[route](reqAbs),nil

}

func RouteExists(route string) (bool) {

	// iterate over each route
	for k,_ := range Routes {

		if route == k {
			return true
		}
	}

	return false
}

func GetRouteName(reqAbs model.RequestAbstract) (string) {

	route := strings.ToLower(reqAbs.Application + constant.HECTOR_ROUTE_DELIMITER + reqAbs.Action + constant.HECTOR_ROUTE_DELIMITER + reqAbs.HTTPRequestType)

	return route
}
