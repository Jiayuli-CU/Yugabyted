package driver

import (
	"fmt"
	"testing"
)

func Test(t *testing.T) {
	filePath := "../data/xact_files"
	for i := 0; i < 20; i++ {
		SqlClient(fmt.Sprintf("%v/%v.txt", filePath, i), 0)
	}
}
