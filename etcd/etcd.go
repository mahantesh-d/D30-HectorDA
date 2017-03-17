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
	var Port = config.Get().Hector.Port
	for {
		time.Sleep(time.Duration(constant.EtcdMessageInterval) * time.Second)

		cfg := client.Config{
			Endpoints: constant.EtcdEndpoints,
			Transport: client.DefaultTransport,
			// set timeout per request to fail fast when the target endpoint is unavailable
			HeaderTimeoutPerRequest: time.Second,
		}

		c, err := client.New(cfg)

		if err != nil {
			logger.Write("ERROR", "Could not connect to Etcd : "+err.Error())
		}

		kapi := client.NewKeysAPI(c)
		logger.Write("DEBUG", "Sending Hearbeat")
		_, err = kapi.Set(context.Background(), constant.EtcdHeartbeatDirectory+"/"+utils.ExecuteCommand("hostname", "-i")+":"+Port, "alive", &client.SetOptions{TTL: time.Duration(constant.EtcdTTL) * time.Second})

		if err != nil {
			logger.Write("ERROR", "Could not send hearbeat : "+err.Error())
		} else {

		}
	}
}
