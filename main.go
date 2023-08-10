package main

import (
	handler "simpredis/redis/server"
	"simpredis/server"
	"simpredis/utils/config"
	"simpredis/utils/logger"
)

func main() {
	config.LoadConfig("simpredis.conf")
	logger.Init()
	server.ListenAndServe(config.Cfg, handler.NewHandler())
}