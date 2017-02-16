package model

// RequestAbstract acts a wrapper which used to ensure standard communication with underlying modules
type RequestAbstract struct {
	Application     string                 // end application ( this would be specific to the domain )
	Action          string                 // action ( what module within this application is being invoked )
	HTTPRequestType string                 // HTTP Request Type ( GET or POST )
	Payload         map[string]interface{} // Payload ( Data to be sent to the server for insertion )
	Filters         map[string]string      // Filters ( query parameters for fetching data  from the server )
}
