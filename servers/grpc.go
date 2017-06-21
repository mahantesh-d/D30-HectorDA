package servers

import (
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/model"
	"github.com/dminGod/D30-HectorDA/proto_types/pb"
	"github.com/dminGod/D30-HectorDA/utils"
	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"net"
	"strconv"
	"encoding/json"
	"time"
)

// GRPCServer registers
type GRPCServer struct{}

// AtomicAdd : empty stub
func (g *GRPCServer) AtomicAdd(ctx context.Context, req *pb.Request) (*pb.Response, error) {

	return new(pb.Response), nil
}

// Do is used to perform simple RPC communication to store and query data from the endpoints
func (g *GRPCServer) Do(ctx context.Context, req *pb.Request) (*pb.Response, error) {

	resp := new(pb.Response)

	if validGRPCRequest(req, resp) {

		// map the data to the abstract request
		// Changed this from a globabl variable to a local one so its fresh
		// across requests.
		RequestAbstract := mapGRPCAbstractRequest(req)

		// routing
		respAbs, _ := HandleRoutes(RequestAbstract)

		// map the result to abstract response
		resp = mapAbstractResponse(respAbs, req)
	}
	
	return resp, nil
}

// GetStream : empty stub
func (g *GRPCServer) GetStream(req *pb.Request, streamResp pb.D21_GetStreamServer) error {

	return nil
}

// ResolveAlias : empty stub
func (g *GRPCServer) ResolveAlias(ctx context.Context, req *pb.Request) (*pb.Response, error) {

	return new(pb.Response), nil
}

// TxBegin : empty stub
func (g *GRPCServer) TxBegin(ctx context.Context, req *pb.TxBeginRequest) (*pb.TxBeginResponse, error) {

	return new(pb.TxBeginResponse), nil
}

// TxDo : empty stub
func (g *GRPCServer) TxDo(ctx context.Context, req *pb.Request) (*pb.Response, error) {

	return new(pb.Response), nil
}

// TxCommit : empty stub
func (g *GRPCServer) TxCommit(ctx context.Context, req *pb.TxCommitRequest) (*pb.TxCommitResponse, error) {

	return new(pb.TxCommitResponse), nil
}

// TxRollback : empty stub
func (g *GRPCServer) TxRollback(ctx context.Context, req *pb.TxRollbackRequest) (*pb.TxRollbackResponse, error) {

	return new(pb.TxRollbackResponse), nil
}

// GRPCStartServer starts the grpc server on the configured port
func GRPCStartServer() {
	Conf = config.Get()

	// listen to the TCP port
	logger.Write("INFO", "Server Starting - host:port - "+utils.ExecuteCommand("hostname", "-i")+" : "+ Conf.Hector.Port)
	listener, err := net.Listen(Conf.Hector.ConnectionType, utils.ExecuteCommand("hostname", "-i")+":"+ Conf.Hector.Port)

	if err != nil {
		logger.Write("ERROR", "Server Starting Fail - host:port - "+Conf.Hector.Host+" : "+ Conf.Hector.Port)
		utils.AppExit("Exiting app, configured port not available")
	} else {
		logger.Write("INFO", "Server Running - host:port - "+Conf.Hector.Host+" : "+ Conf.Hector.Port)
	}

	// Refresh the config in case there are changes from ETCD
	go func(){

		for {
			time.Sleep(10 * time.Second)
			Conf = config.Get()
		}
	}()


	grpcServer := grpc.NewServer()
	pb.RegisterD21Server(grpcServer, new(GRPCServer))
	grpcServer.Serve(listener)
}




func mapGRPCAbstractRequest(req *pb.Request) model.RequestAbstract {

	var reqAbs model.RequestAbstract

	reqAbs.Application = req.GetApplicationName()
	reqAbs.APIVersion = req.GetApplicationVersion()
	reqAbs.Action = req.GetApplicationMethod()
	reqAbs.ID = req.GetID()


	// Set the default limit
	defaultLimit, err := strconv.Atoi(Conf.Hector.DefaultRecordsLimit)

	// Set the limit to 20 if the limit is not set on the configuration
	if err != nil {

		logger.Write("ERROR", "No defaultRecordLimit set in config.toml file, using 20 records as default")
		defaultLimit = 20
	}

	// Set default first, if passed, then will be increased
	reqAbs.Limit = uint32(defaultLimit)
	reqAbs.Offset = 0

	reqAbs.HTTPRequestType = req.GetMethod().String()

	if reqAbs.HTTPRequestType == "POST" || reqAbs.HTTPRequestType == "PUT" {

		reqAbs.Payload = utils.DecodeJSON(req.GetApplicationPayload())
		reqAbs.Filters, reqAbs.IsOrCondition = utils.ParseFilter(req.GetFilter())
		reqAbs.ComplexFilters = req.GetFilter()

		logger.Write("INFO", "GRPC, got filters " + req.GetFilter() )

	} else if reqAbs.HTTPRequestType == "GET" {

		reqAbs.Filters, reqAbs.IsOrCondition = utils.ParseFilter(req.GetFilter())
		reqAbs.ComplexFilters = req.GetFilter()
		reqAbs.TableFields = utils.ParseSelectFields(req.GetSelectField())

		if req.GetRowLimit() > 0 {

			tmpNum, _ := strconv.Atoi(Conf.Hector.MaxLimitAllowedByAPI)
			maxAllowedLimit := uint32(tmpNum)


			reqAbs.Limit = req.GetRowLimit()

			if maxAllowedLimit > 0 && req.GetRowLimit() > maxAllowedLimit {

				reqAbs.Limit = uint32(maxAllowedLimit)
			}


		}

		if req.GetOffset() > 0 {
			reqAbs.Offset = req.GetOffset()
		}
	}

	logger.Write("INFO", "This is the reqAbs object", reqAbs)

	return reqAbs
}



func mapAbstractResponse(respAbs model.ResponseAbstract, reqAbs *pb.Request) *pb.Response {

	resp := new(pb.Response)
	resp.StatusCode = *(proto.Uint32(uint32(float64(respAbs.StatusCode))))
	resp.Status = respAbs.Status
	resp.StatusCodeMessage = respAbs.StandardStatusMessage
	resp.Message = respAbs.Text
	resp.Data = respAbs.Data
	resp.Count = *(proto.Uint64(respAbs.Count))
	resp.ID = reqAbs.GetID()

	resObjBytes, _ := json.Marshal(resp)
	logger.Write("INFO", "Gave Response GRPC" + string(resObjBytes) )

	return resp
}

/*
	Maps the gRPC request with the internal RequestAbstract

	If the request type is POST or PUT then checks if the data sent is valid
	or not if the JSON is not valid, update the response for a fail response and
	return false

*/

func validGRPCRequest(req *pb.Request, resp *pb.Response) bool {

	var reqAbs model.RequestAbstract

	reqAbs.Application = req.GetApplicationName()
	reqAbs.Action = req.GetApplicationMethod()
	reqAbs.HTTPRequestType = req.GetMethod().String()
	route := GetRouteName(reqAbs)
	reqAbs.ID = req.GetID()

	// check if the route exists
	logger.Write("INFO","Route : " + route)

	// post validations
	if req.GetMethod().String() == "POST" || req.GetMethod().String() == "PUT" {

		if !utils.IsJSON(req.GetApplicationPayload()) {

			resp.StatusCode = 400
			resp.Status = "fail"
			resp.StatusCodeMessage = "INVALID_PARAMETERS"
			resp.Message = "The parameters are invalid"
			resp.Data = "{}"
			resp.ID = req.GetID()
			resp.Count = 0
			return false

		}
	}

	return true
}
