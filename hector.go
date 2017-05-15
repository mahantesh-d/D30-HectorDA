package main

import (
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/servers"
	"github.com/dminGod/D30-HectorDA/utils"
	"fmt"
	"github.com/dminGod/D30-HectorDA/logger"
)

// This is the first function that gets called. It initializes configuration and starts all the types of servers
// that are mentioned in the config -- StartServersOfType

func main() {

	// Initialize the Utils
	utils.Init()
	config.Init()

	logger.Write("INFO", "Hector initialize called, Version : "+ config.Conf.Hector.Version)

//	alltrade.ProductMasterGet(model.RequestAbstract{})

	// Start the servers based on the configuration
	conf := config.Get()

	for i, server := range conf.Hector.StartServersOfType {

		i++

		fmt.Println("Calling server start for ", server)
		servers.Server(server)
		fmt.Println("This is getting called..")
	}


}
