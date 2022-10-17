package cassandra

import (
	"fmt"
	"github.com/gocql/gocql"
	"log"
)

var session *gocql.Session

func init() {
	var err error
	cluster := gocql.NewCluster("ap-southeast-1.cffa655e-246b-4910-bb38-38d762998390.aws.ybdb.io")
	cluster.Keyspace = "yugabyte"
	cluster.Consistency = gocql.Quorum
	cluster.ProtoVersion = 4
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "admin",
		Password: "SYl-f5R-0HM69wk1U0FLjLfPd3ziNx",
	}
	//cluster.PoolConfig.HostSelectionPolicy = gocql.DCAwareRoundRobinPolicy("ap-southeast-1")
	cluster.SslOpts = &gocql.SslOptions{
		CaPath:                 "../root.crt",
		EnableHostVerification: false,
	}

	session, err = cluster.CreateSession()
	if err != nil {
		panic(err)
	} else {
		fmt.Println("successfully connected to ycql database")
	}
	defer session.Close()

	// create keyspaces
	err = session.Query("CREATE KEYSPACE IF NOT EXISTS yugabyte WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};").Exec()
	if err != nil {
		log.Println(err)
		return
	}
}

func GetSession() *gocql.Session {
	return session
}
