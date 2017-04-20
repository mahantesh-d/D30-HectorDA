package constant

// EtcdConnectionURL is the Connection URL to fetch configuration from etcd
const EtcdConnectionURL string = "http://10.138.32.217:2379"

// EtcdEndpoints is the list of contact points in the etcd cluster
var EtcdEndpoints = []string{ "http://10.138.32.217:2379","http://10.138.32.218:2379", "http://10.138.32.219:2379",
	"http://10.138.32.220:2379","http://10.138.32.221:2379","http://10.138.32.222:2379",  }

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