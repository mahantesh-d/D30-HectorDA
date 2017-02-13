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

		// All trade version 1
		"alltrade_stock_adjustment_post" : alltrade.StockAdjustment_Post,
		"alltrade_stock_adjustment_get" : alltrade.StockAdjustment_Get,
		"alltrade_obtain_detail_post" : alltrade.ObtainDetail_Post,
		"alltrade_substock_detail_transfer_post" : alltrade.SubStockDetailTransfer_Post,
		"alltrade_substock_daily_detail_post" : alltrade.SubStockDailyDetail_Post,
		"alltrade_transferout_mismatch_post" : alltrade.TransferOutMismatch_Post,
		"alltrade_requestgoods_post" : alltrade.RequestGoods_Post,
		"alltrade_ordertransfer_post" : alltrade.OrderTransfer_Post,
		"alltrade_saleout_detail_post" : alltrade.SaleOutDetail_Post,
		"alltrade_checkstock_detail_post" : alltrade.CheckStockDetail_Post        }
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
