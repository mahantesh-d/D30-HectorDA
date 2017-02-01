package utils

import (
	"github.com/dminGod/D30-HectorDA/proto_types/Msg"
	"net"
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/endpoint"
	"github.com/golang/protobuf/proto"
	"encoding/json"
)
var Message *Msg.Msg 
func init() {

	Message = new(Msg.Msg)
}
func ProtoParseMsg(conn *net.Conn) {

	// close the connection once the function exits
	defer (*conn).Close()

	// make a byte array of capacity 4096	
	data := make([]byte, 4096)
 
	//Read the data waiting on the connection and put it in the data buffer
 	n,err:= (*conn).Read(data)

	if err != nil {
		logger.Write("ERROR", err.Error(), Conf.Hector.Log)
	} 
 	
	//Convert all the data retrieved into the ProtobufTest.TestMessage struct type
 	err = proto.Unmarshal(data[0:n], Message)

	logger.Write("INFO","Decoding Protobuf Message", Conf.Hector.Log)
	
	// decode payload
	ProtoDecodeMsg(conn, Message)

	endpoint.Process(conn, &Conf, &HectorSession)

}

func ProtoDecodePayload(input interface{}) map[string]interface{} {
	
	var payload interface{}

	err := json.Unmarshal([]byte(input.(string)),&payload)
	
	if err != nil {
		logger.Write("ERROR",err.Error(),Conf.Hector.Log)
	}

	return payload.(map[string]interface{})
	
}


func ProtoDecodeMsg(conn *net.Conn, msg *Msg.Msg) {

	HectorSession.Method = msg.GetMethod()
	HectorSession.Module = msg.GetModule()
	HectorSession.Endpoint = msg.GetEndpoint()
	HectorSession.Payload = ProtoDecodePayload(msg.GetPayload())
	HectorSession.Connection = *conn

	logger.Write("DEBUG", HectorSession.Module, Conf.Hector.Log)
}
