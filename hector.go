package main

import (
	"github.com/dminGod/D30-HectorDA/servers"
	"github.com/dminGod/D30-HectorDA/utils"
)

func main() {

	utils.Init()
	// Start the Hector Server
	servers.Server("grpc")

}
