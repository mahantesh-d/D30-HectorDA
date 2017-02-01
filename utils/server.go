package utils

import(
	"net"
	"github.com/dminGod/D30-HectorDA/logger"
)

// used to start a TCP server
func Server () {
        
        // listen to the TCP port
        logger.Write("INFO","Starting Server on " + Conf.Hector.Host + ":" + Conf.Hector.Port, Conf.Hector.Log)
	listener, _ := net.Listen(Conf.Hector.ConnectionType, Conf.Hector.Host + ":" + Conf.Hector.Port)
	logger.Write("INFO", "==== Server Running on "+ Conf.Hector.Host + ":" + Conf.Hector.Port + " =======", Conf.Hector.Log)
 
	for{
                if conn, err := listener.Accept(); err == nil{
                        // if err is nil then that means that data is available for us so we move ahead
                        go handleConnection(&conn)
                } else{
                        continue
                }
        }
}

func handleConnection(conn *net.Conn) {
	
	ProtoParseMsg(conn)	
	
}
