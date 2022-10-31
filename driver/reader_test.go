package driver

import (
	"fmt"
	"github.com/goccy/go-json"
	"testing"
	"time"
)

func TestSqlClient(t *testing.T) {
	filePath := "../data/test_xact_files/test.txt"
	CqlClient(filePath, 0)
}

func TestSqlClient2(t *testing.T) {

	type test struct {
		A int       `json:"a,omitempty"`
		T time.Time `json:"t,omitempty"`
	}

	timer := test{A: 1}
	j, _ := json.Marshal(timer)
	fmt.Println(string(j))

}
