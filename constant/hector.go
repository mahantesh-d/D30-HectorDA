package constant

// HectorPipe is the Named pipe used to listen for graceful server shutdown
const HectorPipe = "/tmp/hector"

// HectorConf
const HectorConf = "conf-example"

// HectorGrpcMode is the GRPC server mode
const HectorGrpcMode string = "grpc"

// HectorProtobufMode is the native protobuf server mode
const HectorProtobufMode string = "protobuf"

// HectorRouteDelimiter is the delimiter used for route mapping
const HectorRouteDelimiter = "_"
