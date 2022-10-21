package driver

import (
	"fmt"
	"strconv"
	"testing"
)

func TestSqlClient(t *testing.T) {
	filePath := "../data/test_xact_files/test.txt"
	SqlClient(filePath, 0)
}

func TestSqlClient2(t *testing.T) {
	s := "5"
	i, _ := strconv.Atoi(s)
	fmt.Println(s, i)
}
