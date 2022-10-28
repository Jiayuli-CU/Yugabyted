package driver

import (
	"fmt"
	"testing"
)

func TestSqlClient(t *testing.T) {
	filePath := "../data/test_xact_files/test.txt"
	SqlClient(filePath, 0)
}

func TestSqlClient2(t *testing.T) {
	s := make([]int, 10)
	for i, t := range s[3:] {
		fmt.Println(i, t)
	}
}
