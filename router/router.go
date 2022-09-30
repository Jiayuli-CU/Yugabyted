package router

import (
	"cs5424project/router/postgre"
	"github.com/gin-gonic/gin"
)

func RegisterRouter(r *gin.Engine) {

	postgreStyle := r.Group("/sql")

	postgreStyle.POST("/order", postgre.Order)
}
