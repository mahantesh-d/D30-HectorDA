package servers

import (
	"github.com/dminGod/D30-HectorDA/model"
	"strings"
	"time"
	"fmt"
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/utils"
	"regexp"
)

func enrichAllRequests(reqAbs *model.RequestAbstract) {

	// Make sure its in upper case cause we're checking against it later
	reqAbs.HTTPRequestType = strings.ToUpper(reqAbs.HTTPRequestType)

	// capture request time
	reqAbs.RequestTime = time.Now().UnixNano() / int64(time.Millisecond)

	convertIncomingTimes(&reqAbs)
}



func convertIncomingTimes(reqAbs **model.RequestAbstract){

	if (**reqAbs).HTTPRequestType == "POST" || (**reqAbs).HTTPRequestType == "PUT" {

		for kk, vv := range (**reqAbs).Payload {

			columnDetails := utils.GetColumnDetailsGeneric("apiName", (**reqAbs).RouteName, kk)

			if len(columnDetails) > 0 {
			columnType := columnDetails["type"].(string)

			if columnType == "timestamp" {

				parsedTime := matchTimeFromString(vv.(string))

				if len(parsedTime) > 6 {

					fmt.Println("Time got parsed from native format : ", vv, "to ", parsedTime)
					(**reqAbs).Payload[kk] = parsedTime
				} else {

					(**reqAbs).Payload[kk] = ""
				}

			}

			}
		}


	} else if (**reqAbs).HTTPRequestType == "GET" {


	}
}



func matchTimeFromString(timePassed string) string {

	var retString = ""


	re := regexp.MustCompile(`(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})\+?(\d{4})?`)
	segs := re.FindAllStringSubmatch(timePassed, -1)


	if len(segs) > 0 {

		retString = segs[0][1] + "-" + segs[0][2] + "-" + segs[0][3] + " " + segs[0][4] + ":" + segs[0][5] + ":" + segs[0][6] + "+" + segs[0][7]
	}

	return retString
}


func enrichAllResponses(resAbs *model.ResponseAbstract) {
	
	// capture response time
	resAbs.ResponseTime = time.Now().UnixNano() / int64(time.Millisecond)
	
	// calculate total time taken for the request
	resAbs.RequestTotalTime = resAbs.ResponseTime - resAbs.RequestAbs.RequestTime

	resAbsRefined := *resAbs
	resAbsRefined.RequestAbs.Payload = make(map[string]interface{})
	resAbsRefined.Data = "" 
	logger.Metric(fmt.Sprintln(resAbsRefined))
}
