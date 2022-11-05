package driver

import (
	"fmt"
	"testing"
)

func Test(t *testing.T) {
	SqlClient(nil, fmt.Sprintf("../data/xact_files/2.txt"), 0)
}
