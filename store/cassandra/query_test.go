package cassandra

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_Query(t *testing.T) {
	var err error
	err = QueryTest()
	assert.NoError(t, err)
}
