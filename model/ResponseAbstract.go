package model

// ResponseAbstract acts as wrapper which is used to map protocol specific response formats
type ResponseAbstract struct {
	Status                string          // status ( success / fail )
	StatusCode            int             // status code ( similar to http status codes 200 OK / 404 Not Found )
	StandardStatusMessage string          // program readable status response message
	Text                  string          // human readable status response message
	Data                  string          // response data
	Count                 uint64          // count of the response data in terms of number of rows
	RequestAbs            RequestAbstract // embed the request object in the response, so if need something we have it here.
	ResponseTime	      int64	      // capture the time when the response is prepated
	RequestTotalTime      int64           // capture the total time taken by request
}
