package model

type cassandra struct {
	Host string
	Port string
}

type hector struct {
	ConnectionType string
	Version string
	Host string
	Port string
	Log string
}

type Config struct {
	Cassandra cassandra
	Hector hector
}
