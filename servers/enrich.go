package servers

import (
	"github.com/dminGod/D30-HectorDA/model"
	"strings"
)

func enrichAllRequests(reqAbs *model.RequestAbstract) {

	// Make sure its in upper case cause we're checking against it later
	reqAbs.HTTPRequestType = strings.ToUpper(reqAbs.HTTPRequestType)
}

func enrichAllResponses(resAbs *model.ResponseAbstract) {

}
