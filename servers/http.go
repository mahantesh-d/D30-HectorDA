package servers

import (
	"fmt"
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/model"
	"github.com/dminGod/D30-HectorDA/utils"
	"io/ioutil"
	"net/http"
	"strings"
)

var HttpServer *http.ServeMux

func HTTPStartServer() {
	Conf = config.Get()
	HttpServer = http.NewServeMux()
	handleHttpRoutes()
	logger.Write("INFO", "Server Starting - host:port - "+Conf.Hector.Host+" : "+Conf.Hector.Port)
	err := http.ListenAndServe(Conf.Hector.Host+":"+Conf.Hector.Port, HttpServer)
	if err != nil {
		logger.Write("ERROR", "Server Starting Fail - host:port - "+Conf.Hector.Host+" : "+Conf.Hector.Port)
		utils.AppExit("Exiting app, configured port not available")
	} else {
		logger.Write("INFO", "Server Running - host:port - "+Conf.Hector.Host+" : "+Conf.Hector.Port)
	}
}

func handleHttpRoutes() {

	fmt.Println("Handling")
	HttpServer.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var response string
		if validHTTPRequest(r, &response) {
			RequestAbstract = mapHTTPAbstractRequest(r)
			resp, _ := HandleRoutes(RequestAbstract)
			response = utils.EncodeJSON(resp)
		}
		fmt.Println("Printing")

		fmt.Fprintln(w, response)

	})
}

func validHTTPRequest(r *http.Request, response *string) bool {
	fmt.Println("Validating")
	applicationConfig := strings.Split(r.URL.Path, "/")
	var reqAbs model.RequestAbstract
	resp := make(map[string]interface{})
	fmt.Println("Made map of string and interface")
	if len(applicationConfig) != 4 {
		resp["StatusCode"] = 404
		resp["Status"] = "fail"
		resp["StatusCodeMessage"] = "NOT_FOUND"
		resp["Message"] = "The given route was not found"
		resp["Data"] = "{}"
		resp["Count"] = 0
		*response = utils.EncodeJSON(resp)
		fmt.Println("returning false")
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
		if err != nil {
			fmt.Println(err.Error())
		}

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
	reqAbs.Application = applicationConfig[2]
	reqAbs.Action = applicationConfig[3]
	reqAbs.HTTPRequestType = r.Method
	if reqAbs.HTTPRequestType == "POST" {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			fmt.Println(err.Error())
		}
		reqAbs.Payload = utils.DecodeJSON(string(body))
	} else if reqAbs.HTTPRequestType == "GET" {
		params := r.URL.Query()
		if len(params["filters"]) > 0 {
			reqAbs.Filters = utils.ParseFilter(params["filters"][0])
		}
	}

	return reqAbs

}

func mapHTTPAbstractResponse() {

}
