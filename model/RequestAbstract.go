package model

type RequestAbstract struct {
	Application string
	Action   string
	HTTPRequestType string
	Payload    map[string]interface{}
}
