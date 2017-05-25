package model

// RequestAbstract acts a wrapper which used to ensure standard communication with underlying modules
type RequestAbstract struct {
	Application     string                 		// end application ( this would be specific to the domain )
	Action          string                 		// action ( what module within this application is being invoked )
	HTTPRequestType string                 		// HTTP Request Type ( GET or POST )
	Payload         map[string]interface{} 		// Payload ( Data to be sent to the server for insertion )
	Filters         map[string]interface{}    	// Filters ( query parameters for fetching data  from the server )
	RequestProtocol string                 		// Did this request come over "http", "protobuf"
	AdditionalData  map[string]interface{} 		// This is extra data related to a request, used by specific methods
	APIVersion      uint32                 		// This is the version of the API
	RouteName       string                 		// This is the route taken for the request
	RequestTime	int64		       		// This is the time when the request was made
	ID              uint64		       		// This is the request ID that will be sent by calling application, we just need to return it back...
	Limit 		int32
	Token		string
	IsOrCondition	bool

	DatabaseType	string
	DatabaseName	string
	Table		string
	ApiName		string
	TableFields	[]Field
	IsPutSupported	bool
//	RequestFilters 	[]RequestFilter

	SearchFilter	[]Field
	SelectFields	[]Field
	BodyFields	[]Field
}


type Field struct {

	FieldName 		string
	ColumnType		string
	ColumnName		string
	IsMultiValue		bool 	// single or multi
	Tags            	[]string
	IsGetField		bool
	IsPutField		bool
	IsPutFilterField	bool

	Value 			interface{}
	Role			string	// search_filter : will have a value and a value type,
					// select : will have a key and value type(no value),
					// body_field : will have key, value and value type

	ValueGoType 		string
}


/*
type RequestFilter struct {

	FilterUID	string
	FilterType 	string
	Key		string
	Value		string
	FieldData	map[string]string
	FieldTags	[]string
}

type FilterCondition struct {

	FilterType	string
	FilterFields	[]RequestFilter
	FilterCondition	[]*FilterCondition
}

*/



