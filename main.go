package main

import (
	handler "github.com/HK40404/simpredis/redis/server"
	"github.com/HK40404/simpredis/server"
	"github.com/HK40404/simpredis/utils/config"
	"github.com/HK40404/simpredis/utils/logger"
)

func Initialize() {
	config.LoadConfig("simpredis.conf")
	logger.Init()
}

func main() {
	Initialize()
	server.ListenAndServe(config.Cfg, handler.NewHandler())
}
