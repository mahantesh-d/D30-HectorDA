package servers

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"github.com/dminGod/D30-HectorDA/proto_types/pb"
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/utils"
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/golang/protobuf/proto"
	"net"
	"github.com/dminGod/D30-HectorDA/endpoint"
	"github.com/dminGod/D30-HectorDA/model"
)

type GRPCServer struct {}

func(g *GRPCServer) AtomicAdd(ctx context.Context, req *pb.Request) (*pb.Response,error) {

	return new(pb.Response),nil
}

func(g *GRPCServer) Do(ctx context.Context, req *pb.Request) (*pb.Response,error) {

	resp := new(pb.Response)
	if validRequest(req,resp) {
		// map the data to the abstract request
		RequestAbstract = mapAbstractRequest(req)

		// routing
		respAbs,_ := HandleRoutes(RequestAbstract)

		// map the result to abstract response
		resp = mapAbstractResponse(respAbs)
	}
	return resp,nil
}

func(g *GRPCServer) GetStream(req *pb.Request, stream_resp pb.Hector_GetStreamServer) error {

        return nil
}


func(g *GRPCServer) ResolveAlias(ctx context.Context, req *pb.Request) (*pb.Response,error) {

        return new(pb.Response),nil
}

func(g *GRPCServer) TxBegin(ctx context.Context, req *pb.TxBeginRequest) (*pb.TxBeginResponse,error) {

        return new(pb.TxBeginResponse),nil
}

func(g *GRPCServer) TxDo(ctx context.Context, req *pb.Request) (*pb.Response,error) {

        return new(pb.Response),nil
}


func(g *GRPCServer) TxCommit(ctx context.Context, req *pb.TxCommitRequest) (*pb.TxCommitResponse,error) {

        return new(pb.TxCommitResponse),nil
}


func(g *GRPCServer) TxRollback(ctx context.Context, req *pb.TxRollbackRequest) (*pb.TxRollbackResponse,error) {

        return new(pb.TxRollbackResponse),nil
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
	pb.RegisterHectorServer(grpcServer,new(GRPCServer))
	grpcServer.Serve(listener)
}

func handleEndPoint(Conn *net.Conn, Conf *config.Config, RequestAbstract *model.RequestAbstract) {
	
	var dbAbstract model.DBAbstract

	dbAbstract.DBType = "cassandra"
	dbAbstract.QueryType = "INSERT"
	dbAbstract.Query = "INSERT INTO foo(id, name) VALUES(2,'xyz')"
	

	endpoint.Process(nil,Conf, &dbAbstract)

}

func mapAbstractRequest(req *pb.Request) (model.RequestAbstract) {

	var reqAbs model.RequestAbstract
	reqAbs.Application = req.GetApplicationName()
	reqAbs.Action = req.GetApplicationMethod()
	reqAbs.HTTPRequestType = req.GetMethod().String()
	if reqAbs.HTTPRequestType == "POST" {
		reqAbs.Payload = utils.DecodeJSON(req.GetApplicationPayload())
	} else if reqAbs.HTTPRequestType == "GET" {
		reqAbs.Filters = utils.ParseFilter(req.GetFilter())
	}
	return reqAbs
}

func mapAbstractResponse(respAbs model.ResponseAbstract) (*pb.Response) {

	resp := new(pb.Response)
	resp.StatusCode = *(proto.Uint32(uint32(float64(respAbs.StatusCode))))
	resp.Status = respAbs.Status	
	resp.StatusCodeMessage = respAbs.StandardStatusMessage
	resp.Message = respAbs.Text
	resp.Data = respAbs.Data
	resp.Count =  *(proto.Uint64(respAbs.Count))
	return resp

}

func validRequest(req *pb.Request,resp *pb.Response) bool {

	var reqAbs model.RequestAbstract
	reqAbs.Application = req.GetApplicationName()
	reqAbs.Action = req.GetApplicationMethod()
	reqAbs.HTTPRequestType = req.GetMethod().String()
	route := GetRouteName(reqAbs)
	// check if the route exists
	if !RouteExists(route) {
		resp.StatusCode = 404
		resp.Status = "fail"
		resp.StatusCodeMessage = "NOT_FOUND"
		resp.Message = "The given route was not found"
		resp.Data = "{}"
		resp.Count = 0
		return false	
	}

	// post validations
	if req.GetMethod().String() == "POST" {
		if !utils.IsJSON(req.GetApplicationPayload()) {
			resp.StatusCode = 400
			resp.Status = "fail"
			resp.StatusCodeMessage = "INVALID_PARAMETERS"
			resp.Message = "The parameters are invalid"
			resp.Data = "{}"
			resp.Count = 0
			return false
		}
	}

	return true
} 
