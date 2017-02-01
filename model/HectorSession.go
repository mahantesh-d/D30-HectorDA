package model

import(
	"net"
)
type HectorSession struct {

	Connection net.Conn
	Method string
	Module string
	Endpoint string
	Payload map[string]interface{}
}
