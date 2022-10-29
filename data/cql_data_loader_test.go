package data

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func Test_CQLDistrict(t *testing.T) {
	CQLLoadDistrict()
}

func Test_T(t *testing.T) {
	type test struct {
		A int       `json:"a,omitempty"`
		T time.Time `json:"t,omitempty"`
	}

	timer := test{A: 1}
	j, _ := json.Marshal(timer)
	fmt.Println(string(j))

}
