package main

import (
	handler "simpredis/redis/server"
	"simpredis/server"
	"simpredis/utils/config"
	"simpredis/utils/logger"
)

func Initialize() {
	config.LoadConfig("simpredis.conf")
	logger.Init()
}

func main() {
	Initialize()
	server.ListenAndServe(config.Cfg, handler.NewHandler())
}