package main

import (
	"cs5424project/router"
	"github.com/gin-gonic/gin"
)

func main() {
	r := gin.Default()

	router.RegisterRouter(r)

	r.Run()
}
