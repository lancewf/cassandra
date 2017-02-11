package cassandra

import (
	"github.com/gocql/gocql"
)


func BuildCassandraQuery() *StationQuery{
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "test"
	session, _ := cluster.CreateSession()

	return NewStationQuery(session)
}