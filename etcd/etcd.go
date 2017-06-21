package etcd

import (
	"github.com/coreos/etcd/client"
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/constant"
	"github.com/dminGod/D30-HectorDA/logger"
	"github.com/dminGod/D30-HectorDA/utils"
	"golang.org/x/net/context"
	"time"
)

func Heartbeat() {


	for {
		time.Sleep( time.Duration(constant.EtcdMessageInterval) * time.Second )

		var Port = config.Get().Hector.Port

		cfg := client.Config{
			Endpoints: constant.EtcdEndpoints,
			Transport: client.DefaultTransport,
			// set timeout per request to fail fast when the target endpoint is unavailable
			HeaderTimeoutPerRequest: time.Second,
		}

		c, err := client.New(cfg)

		if err != nil {
			logger.Write("INFO", "Could not connect to Etcd : "+err.Error())
		}

		kapi := client.NewKeysAPI(c)
		logger.Write("VERBOSE", "Sending Hearbeat")

		_, err = kapi.Set(context.Background(),
		"/damocles_cfg_type_cwf_cluster_01_" + utils.ExecuteCommand("hostname", "-i") + "_" + Port,
			utils.ExecuteCommand("hostname", "-i") + ":" + Port, // Value
			&client.SetOptions{ Dir: false, TTL: time.Duration(constant.EtcdTTL) * time.Second} )

		if err != nil {
			logger.Write("VERBOSE", "Could not send hearbeat : "+err.Error())
		} else {

		}
	}
}
