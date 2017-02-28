package main

import (
	"github.com/dminGod/D30-HectorDA/config"
	"github.com/dminGod/D30-HectorDA/servers"
	"github.com/dminGod/D30-HectorDA/utils"
)

// This is the first function that gets called. It initializes configuration and starts all the types of servers
// that are mentioned in the config -- StartServersOfType

func main() {

	// Initialize the Utils
	utils.Init()

	// Start the servers based on the configuration
	conf := config.Get()

	for _, server := range conf.Hector.StartServersOfType {

		servers.Server(server)
	}
}
