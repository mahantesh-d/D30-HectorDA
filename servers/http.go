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
)

// HttpServer is the http server handler
var HttpServer *http.ServeMux

// HTTPStartServer starts the HTTP Server on the configured port
func HTTPStartServer() {
	Conf = config.Get()
	HttpServer = http.NewServeMux()
	handleHTTPRoutes()
	logger.Write("INFO", "Server Starting - host:port - "+Conf.Hector.Host+" : "+Conf.Hector.Port)
	err := http.ListenAndServe(Conf.Hector.Host+":"+Conf.Hector.Port, HttpServer)
	if err != nil {
		logger.Write("ERROR", "Server Starting Fail - host:port - "+Conf.Hector.Host+" : "+Conf.Hector.Port)
		utils.AppExit("Exiting app, configured port not available")
	} else {
		logger.Write("INFO", "Server Running - host:port - "+Conf.Hector.Host+" : "+Conf.Hector.Port)
	}
}

func handleHTTPRoutes() {

	HttpServer.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var response string
		if validHTTPRequest(r, &response) {
			RequestAbstract = mapHTTPAbstractRequest(r)
			resp, _ := HandleRoutes(RequestAbstract)
			response = utils.EncodeJSON(resp)
		}

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
	route := GetRouteName(reqAbs)
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

	if r.Method == "POST" {
		body, err := ioutil.ReadAll(r.Body)
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
		params := r.URL.Query()
		if len(params["filters"]) > 0 {
			reqAbs.Filters = utils.ParseFilter(params["filters"][0])
		}
		reqAbs.Limit = 0
		reqAbs.Token = ""
		if len(params["limit"]) > 0 {
			reqAbs.Limit = 10
		}
		if len(params["token"]) > 0  {
			reqAbs.Token = params["token"][0]
		}
	}

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
