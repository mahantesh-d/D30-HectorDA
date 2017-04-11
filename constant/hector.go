package constant

// HectorPipe is the Named pipe used to listen for graceful server shutdown
const HectorPipe = "/tmp/hector"

// TODO: 1) Change this to a better folder name -- 2) All the configuration should be outside the binary and in one place

// HectorConf is the path of the configuration file
const HectorConf = "/etc/hector"
// const HectorConf = "conf-example"

// HectorGrpcMode is the GRPC server mode
const HectorGrpcMode string = "grpc"

// HectorProtobufMode is the native protobuf server mode
const HectorProtobufMode string = "protobuf"

// HTTP is the HTTP server mode
const HTTP string = "http"

// HectorRouteDelimiter is the delimiter used for route mapping
const HectorRouteDelimiter = "_"
