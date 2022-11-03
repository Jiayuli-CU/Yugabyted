package cassandra

import (
	"fmt"
	"github.com/gocql/gocql"
	"log"
	"time"
)

var session *gocql.Session

const (
	keySpace = "cs5424_groupI"
	host1    = "ap-southeast-1.fbe2e2ee-644d-441a-8bc0-61a134b3f1af.aws.ybdb.io"
	host2    = "192.168.48.244:9040"
	password = "lZdcAJFv1BlkhUMsiz86dLSV-Z1__h"
)

func init() {
	var err error
	cluster := gocql.NewCluster("192.168.48.244:9042", "192.168.48.245:9042", "192.168.48.246:9042", "192.168.48.247:9042", "192.168.48.248:9042")
	//cluster.Keyspace = keySpace
	//cluster := gocql.NewCluster("ap-southeast-1.fbe2e2ee-644d-441a-8bc0-61a134b3f1af.aws.ybdb.io")
	cluster.Consistency = gocql.Quorum
	cluster.ProtoVersion = 4
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "yugabyte",
		Password: "yugabyte",
	}
	//cluster.PoolConfig.HostSelectionPolicy = gocql.DCAwareRoundRobinPolicy("ap-southeast-1")
	//cluster.SslOpts = &gocql.SslOptions{
	//	CaPath:                 "cassandra_root.crt",
	//	EnableHostVerification: false,
	//}
	cluster.Timeout = time.Minute

	session, err = cluster.CreateSession()
	if err != nil {
		panic(err)
	} else {
		fmt.Println("successfully connected to ycql database")
	}
	//defer session.Close()

	// create keyspaces
	err = session.Query("CREATE KEYSPACE IF NOT EXISTS cs5424_groupI WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};").Exec()
	if err != nil {
		log.Println(err)
		return
	}

	createSchema()
}

func GetSession() *gocql.Session {
	return session
}

func CloseSession() {
	session.Close()
}
