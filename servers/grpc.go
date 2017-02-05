package servers

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"github.com/dminGod/D30-HectorDA/proto_types/GRpc"
	"github.com/golang/protobuf/proto"	
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/utils"
	"github.com/dminGod/D30-HectorDA/config"
	"net"
	"github.com/dminGod/D30-HectorDA/endpoint"
	"github.com/dminGod/D30-HectorDA/model"
)

type GRPCServer struct {}

func(g *GRPCServer) Execute(ctx context.Context, msgReq *Grpc.MsgRequest) (*Grpc.MsgResponse,error) {

	endpoint := proto.String(msgReq.GetEndpoint())
	method := msgReq.GetMsgmethod().String()
	module := proto.String(msgReq.GetModule())
	payload := proto.String(msgReq.GetPayload())
	logger.Write("DEBUG", "Endpoint is " + *endpoint)
	logger.Write("DEBUG", "Method is " + method)
	logger.Write("DEBUG", "Module is " + *module)
	logger.Write("DEBUG", "Payload is " + *payload)

	HectorSession.Method = method
	HectorSession.Module = *module
 	HectorSession.Endpoint = *endpoint
	HectorSession.Payload = ProtoDecodePayload(*payload)

	output := handleEndPoint(nil,&Conf,&HectorSession)	
	
	msgResponse := responseTransform(output)
	return msgResponse,nil
}


func(g *GRPCServer) ExecuteStream(msgReq *Grpc.MsgRequest, grpcStreamServer Grpc.Hector_ExecuteStreamServer) error {

	return nil
}

func GRPCStartServer() {
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

	grpcServer := grpc.NewServer()
	Grpc.RegisterHectorServer(grpcServer,new(GRPCServer))
	grpcServer.Serve(listener)
}

func handleEndPoint(Conn *net.Conn, Conf *config.Config, HectorSession *model.HectorSession) (model.HectorResponse) {
	output := endpoint.Process(nil,Conf,HectorSession)

	return output
}

func responseTransform(h model.HectorResponse) (*Grpc.MsgResponse) {

	msgResponse := new(Grpc.MsgResponse)
	msgResponse.Status = h.Status
	msgResponse.Message = h.Message
	msgResponse.Data = h.Data

	return msgResponse
}
