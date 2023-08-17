package handler

import (
	"io"
	"net"
	"simpredis/redis/database"
	parser "simpredis/redis/resp"
	"simpredis/utils/logger"
	"sync"
	"sync/atomic"
)

type RedisServer struct {
	conns   sync.Map // 管理连接
	closing atomic.Bool
	engine  *database.DBEngine // 用于操作数据库的manager
}

func (handler *RedisServer) Handle(conn net.Conn) {
	if handler.closing.Load() {
		_ = conn.Close()
		return
	}

	client := NewClient(conn)
	defer client.Close()
	handler.conns.Store(client, struct{}{})

	ch := parser.ParseStream(conn)
	for request := range ch {
		if request.Err != nil {
			if request.Err == io.EOF {
				logger.Info("Connection closed: %s", conn.RemoteAddr().String())
			} else {
				logger.Error("Get error: %v", request.Err)
			}
			return
		}
		if request.Data == nil {
			logger.Error("Parsed empty payload")
			continue
		}

		var reply parser.RespData
		if _, ok := request.Data.(*parser.String); ok {
			// ping inline
			reply = parser.NewString("PONG")
		} else {
			// array command
			array, ok := request.Data.(*parser.Array)
			if !ok {
				logger.Error("command format is not RESP array")
				return
			}
			reply = handler.engine.ExecCmd(array.Args)
		}

		if reply != nil {
			client.Wg.Add(1)
			if _, err := conn.Write(reply.Serialize()); err != nil {
				logger.Error("Fail to send data: %v, closing connection", err)
				client.Wg.Done()
				return
			}
			client.Wg.Done()
		} else {
			client.Wg.Add(1)
			if _, err := conn.Write(parser.NewError("ERR Unknow").Serialize()); err != nil {
				logger.Error("Fail to send data: %v, closing connection", err)
				client.Wg.Done()
				return
			}
			client.Wg.Done()
		}
	}
}

func (handler *RedisServer) Close() {
	// server状态调整为关闭，防止处理新的请求
	handler.closing.Store(true)
	// 优雅关闭：让正在进行的请求处理完毕
	handler.conns.Range(func(key, value any) bool {
		client := key.(*Client)
		client.Close()
		return true
	})
}

func NewHandler() *RedisServer {
	ser := &RedisServer{engine: database.NewDBEngine()}
	ser.closing.Store(false)
	return ser
}
