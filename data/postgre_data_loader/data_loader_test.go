package postgre_data_loader

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_LoadWarehouse(t *testing.T) {
	LoadWarehouse()
}

func Test_LoadOrder(t *testing.T) {
	LoadOrder()
}

func Test_LoadStock(t *testing.T) {
	LoadStock()
}

func Test_LoadDistrict(t *testing.T) {
	LoadDistrict()
}

func Test_LoadCustomer(t *testing.T) {
	LoadCustomer()
}

func Test_LoadItem(t *testing.T) {
	LoadItem()
}

func Test_LoadOrderline(t *testing.T) {
	err := LoadOrderLine()
	assert.NoError(t, err)

}
