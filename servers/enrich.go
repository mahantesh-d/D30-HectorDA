package servers

import (
	"github.com/dminGod/D30-HectorDA/model"
	"strings"
	"time"
	"fmt"
	"github.com/dminGod/D30-HectorDA/logger"
)

func enrichAllRequests(reqAbs *model.RequestAbstract) {

	// Make sure its in upper case cause we're checking against it later
	reqAbs.HTTPRequestType = strings.ToUpper(reqAbs.HTTPRequestType)

	// capture request time
	reqAbs.RequestTime = time.Now().UnixNano() / int64(time.Millisecond)
}

func enrichAllResponses(resAbs *model.ResponseAbstract) {
	
	// capture response time
	resAbs.ResponseTime = time.Now().UnixNano() / int64(time.Millisecond)
	
	// calculate total time taken for the request
	resAbs.RequestTotalTime = resAbs.ResponseTime - resAbs.RequestAbs.RequestTime

	resAbsRefined := resAbs
	resAbsRefined.RequestAbs.Payload = make(map[string]interface{})
	resAbsRefined.Data = "" 
	logger.Metric(fmt.Sprintln(resAbsRefined))
}
