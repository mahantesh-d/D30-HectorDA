package model 

type DBAbstract struct {

	DBType string
	QueryType string
	Query string
	Status string
	Message string
	StatusCodeMessage string
	Data string
	Count uint64
}
