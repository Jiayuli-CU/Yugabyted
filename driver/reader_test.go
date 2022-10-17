package driver

import "testing"

func TestSqlClient(t *testing.T) {
	filePath := "../data/xact_files/0.txt"
	SqlClient(filePath)
}
