package cassandra

import "github.com/gocql/gocql"

var session *gocql.Session

func init() {
	cluster := gocql.NewCluster("host1_ip", "host2_ip", "host3_ip")
	cluster.Keyspace = ""
	cluster.Consistency = gocql.Quorum
	cluster.ProtoVersion = 4
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "",
		Password: "",
	}

	var err error
	session, err = cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()
}

func GetSession() *gocql.Session {
	return session
}
