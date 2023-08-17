package main

import (
	// "container/list"
	// "sync"
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
	// var wg sync.WaitGroup
	// l := list.List{}
	// wg.Add(3)
	// go func () {
	// 	defer wg.Done()
	// 	for i := 0; i < 100; i++ {
	// 		go l.PushBack(i)
	// 	}
	// }()
	// go func () {
	// 	defer wg.Done()
	// 	time.Sleep(50000*time.Nanosecond)
	// 	for v := l.Front(); v != nil; v = v.Next() {
	// 		fmt.Println(v.Value)
	// 	}
	// }()
	// wg.Wait()

}