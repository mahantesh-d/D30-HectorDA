package constant

// EtcdConnectionURL is the Connection URL to fetch configuration from etcd
const EtcdConnectionURL string = "http://localhost:2379"

// EtcdEndpoints is the list of contact points in the etcd cluster
var EtcdEndpoints = []string{"http://localhost:2379"}

// EtcdKey is the Key within etcd server which contains the configuration information
const EtcdKey string = "/hector/config/config.toml"

// EtcdConfigType is the extension of the values in etcd
const EtcdConfigType string = "toml"

// EtcdHeartbeatDirectory is the Key within etcd server which contains the list of active hector instances
const EtcdHeartbeatDirectory string = "/hector/active-servers"

// TTL of heartbeat message in seconds
const EtcdTTL int = 5

// EtcdMessageInterval is the interval of heartbeat
const EtcdMessageInterval int = 3
