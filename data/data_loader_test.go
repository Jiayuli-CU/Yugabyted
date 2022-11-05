package data

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLoadData(t *testing.T) {
	err := LoadWarehouse()
	assert.NoError(t, err)
	err = LoadOrder()
	assert.NoError(t, err)
	err = LoadStock()
	assert.NoError(t, err)
	err = LoadDistrict()
	assert.NoError(t, err)
	err = LoadCustomer()
	assert.NoError(t, err)
	err = LoadItem()
	assert.NoError(t, err)
	err = LoadOrderLine()
	assert.NoError(t, err)
}
