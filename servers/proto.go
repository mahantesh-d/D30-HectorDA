package servers

import (
	"encoding/json"
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/endpoint"
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/utils"
	"github.com/dminGod/D30-HectorDA/proto_types/Msg"
	"github.com/golang/protobuf/proto"
	"net"
)

var Message *Msg.Msg

func init() {

	Message = new(Msg.Msg)
}

func ProtoStartServer() {
	Conf = config.Get();

	// listen to the TCP port
	logger.Write("INFO", "Server Starting - host:port - " + Conf.Hector.Host + " : " + Conf.Hector.Port)
	listener, err := net.Listen(Conf.Hector.ConnectionType, Conf.Hector.Host + ":" + Conf.Hector.Port)

	if err != nil {

        	logger.Write("ERROR", "Server Starting Fail - host:port - " + Conf.Hector.Host + " : " + Conf.Hector.Port )
        	utils.AppExit("Exiting app, configured port not available")

	} else {

        	logger.Write("INFO", "Server Running - host:port - " + Conf.Hector.Host + " : " + Conf.Hector.Port )
	}


	for {
        	if conn, err := listener.Accept(); err == nil {

                	// if err is nil then that means that data is available for us so we move ahead
                		go ProtoParseMsg(&conn)
        		} else {
                		continue
        		}
	}
}

func ProtoParseMsg(conn *net.Conn) {

	// close the connection once the function exits
	defer (*conn).Close()

	// make a byte array of capacity 4096
	data := make([]byte, 4096)

	//Read the data waiting on the connection and put it in the data buffer
	n, err := (*conn).Read(data)

	if err != nil {
		logger.Write("ERROR", err.Error())
	}

	//Convert all the data retrieved into the ProtobufTest.TestMessage struct type
	err = proto.Unmarshal(data[0:n], Message)

	logger.Write("INFO", "Decoding Protobuf Message")

	// decode payload
	ProtoDecodeMsg(conn, Message)

	endpoint.Process(conn, &Conf, &HectorSession)

}

func ProtoDecodePayload(input interface{}) map[string]interface{} {

	var payload interface{}

	err := json.Unmarshal([]byte(input.(string)), &payload)

	if err != nil {
		logger.Write("ERROR", err.Error())
	}

	return payload.(map[string]interface{})

}

func ProtoDecodeMsg(conn *net.Conn, msg *Msg.Msg) {

	HectorSession.Method = msg.GetMethod()
	HectorSession.Module = msg.GetModule()
	HectorSession.Endpoint = msg.GetEndpoint()
	HectorSession.Payload = ProtoDecodePayload(msg.GetPayload())
	HectorSession.Connection = *conn

	output := endpoint.Process(conn,&Conf,&HectorSession)

	_ = output
}
