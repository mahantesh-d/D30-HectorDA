package servers

import (
	"fmt"
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/model"
	"github.com/dminGod/D30-HectorDA/utils"
	"io/ioutil"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"encoding/json"
	"time"
)

// HttpServer is the http server handler
var HttpServer *http.ServeMux

// HTTPStartServer starts the HTTP Server on the configured port
func HTTPStartServer() {
	Conf = config.Get()
	HttpServer = http.NewServeMux()
	handleHTTPRoutes()
	logger.Write("INFO", "Server Starting - host:port - " + Conf.Hector.Host + " : " + Conf.Hector.PortHTTP)


	go func(){

		for {
			time.Sleep(time.Second * 5)
			Conf = config.Get()
		}
	}()



	err := http.ListenAndServe(Conf.Hector.Host+":"+Conf.Hector.PortHTTP, HttpServer)
	if err != nil {
		logger.Write("ERROR", "Server Starting Fail - host:port - "+Conf.Hector.Host+" : "+Conf.Hector.PortHTTP)
		utils.AppExit("Exiting app, configured port not available")
	} else {
		logger.Write("INFO", "Server Running - host:port - "+Conf.Hector.Host+" : "+Conf.Hector.PortHTTP)
	}




}

func handleHTTPRoutes() {

	HttpServer.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var response string
		if validHTTPRequest(r, &response) {
			RequestAbstract = mapHTTPAbstractRequest(r)
			resp, _ := HandleRoutes(RequestAbstract)

			json.Unmarshal([]byte(resp.Data), &resp.DataHTTP)

			resp.Data = ""
			response = utils.EncodeJSON(resp)
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, response)

	})
}

func validHTTPRequest(r *http.Request, response *string) bool {

	applicationConfig := strings.Split(r.URL.Path, "/")

	var reqAbs model.RequestAbstract
	resp := make(map[string]interface{})

	if len(applicationConfig) != 4 {
		resp["StatusCode"] = 404
		resp["Status"] = "fail"
		resp["StatusCodeMessage"] = "NOT_FOUND"
		resp["Message"] = "The given route was not found"
		resp["Data"] = "{}"
		resp["Count"] = 0
		*response = utils.EncodeJSON(resp)

		return false
	}

	reqAbs.Application = applicationConfig[2]
	reqAbs.Action = applicationConfig[3]
	reqAbs.HTTPRequestType = r.Method
	//route := GetRouteName(reqAbs)

	/*
	if !RouteExists(route) {
		resp["StatusCode"] = 404
		resp["Status"] = "fail"
		resp["StatusCodeMessage"] = "NOT_FOUND"
		resp["Message"] = "The given route was not found"
		resp["Data"] = "{}"
		resp["Count"] = 0
		*response = utils.EncodeJSON(resp)
		return false
	}
	*/

	if r.Method == "POST" {
	/*	body, err := ioutil.ReadAll(r.Body)
		utils.HandleError(err)
		if !utils.IsJSON(string(body)) {
			resp["StatusCode"] = 400
			resp["Status"] = "fail"
			resp["StatusCodeMessage"] = "INVALID_PARAMETERS"
			resp["Message"] = "The parameters are invalid"
			resp["Data"] = "{}"
			resp["Count"] = 0
			*response = utils.EncodeJSON(resp)
			return false
		}
		*/
	}

	return true
}

func mapHTTPAbstractRequest(r *http.Request) model.RequestAbstract {
	applicationConfig := strings.Split(r.URL.Path, "/")
	var reqAbs model.RequestAbstract
	reqAbs.APIVersion = parseAPIVersion(applicationConfig[1])
	reqAbs.Application = applicationConfig[2]
	reqAbs.Action = applicationConfig[3]
	reqAbs.HTTPRequestType = r.Method

	if reqAbs.HTTPRequestType == "POST" {

		body, err := ioutil.ReadAll(r.Body)
		utils.HandleError(err)
		reqAbs.Payload = utils.DecodeJSON(string(body))

	} else if reqAbs.HTTPRequestType == "GET" {

//		var params map[string]interface{}

		paramsURI := strings.Split(r.RequestURI, "filter=")

		if len(paramsURI) > 1 {

			reqAbs.Filters, reqAbs.IsOrCondition = utils.ParseFilter(paramsURI[1])
			reqAbs.ComplexFilters = paramsURI[1]
		}


		reqAbs.Limit = 0
		reqAbs.Token = ""

		logger.Write("INFO", "Default Config I have on http is", Conf.Hector.DefaultRecordsLimit)

		limitVal, _ := strconv.Atoi(Conf.Hector.DefaultRecordsLimit)

		reqAbs.Limit = uint32(limitVal)

		/*
		if len(params["limit"]) > 0 {
			reqAbs.Limit = 10
		}
		if len(params["token"]) > 0  {
			reqAbs.Token = params["token"][0]
		} */
	} else if reqAbs.HTTPRequestType == "PUT" {

		body, err := ioutil.ReadAll(r.Body)
		utils.HandleError(err)
		reqAbs.Payload = utils.DecodeJSON(string(body))

		paramsURI := strings.Split(r.RequestURI, "filter=")

		if len(paramsURI) > 1 {

			reqAbs.Filters, reqAbs.IsOrCondition = utils.ParseFilter(paramsURI[1])
			reqAbs.ComplexFilters = paramsURI[1]
		}
	}

	logger.Write("INFO", "This is the reqAbs object", reqAbs)

	return reqAbs
}

func parseAPIVersion(verStr string) uint32 {

	r, _ := regexp.Compile("\\d+")
	singleMatch := r.FindString(verStr)
	retInt, _ := strconv.Atoi(singleMatch)

	return uint32(retInt)
}

func mapHTTPAbstractResponse() {

}
