package model

// DBAbstract acts as a wrapper which communicates with underlying Databases
type DBAbstract struct {
	DBType            string                   // type of the database ( e.g. cassandra, presto etc. )
	QueryType         string                   // type of the query ( e.g. SELECT, INSERT )
	Query             []string                 // query string ( e.g. SELECT * from foo )
	Status            string                   // status string ( e.g. success )
	Message           string                   // human readable message string ( e.g. Queried successfully )
	StatusCodeMessage string                   // program readable status code message string ( e.g. QUERY_SUCCESS )
	Data              string                   // data is the response from the database endpoint
	RichData          []map[string]interface{} // This is the data recieved in the native format from the db
	Count             uint64                   // count of the data
	TableName	  string
	IsOrCondition	  bool
	UpdateCondition	  map[string][]string
}
