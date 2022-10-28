package cassandra

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_Query(t *testing.T) {
	var err error
	QueryTest()
	assert.NoError(t, err)
}
