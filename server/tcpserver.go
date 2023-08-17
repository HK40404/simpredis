package server

import (
	"net"
	"os"
	"os/signal"
	"simpredis/utils/config"
	"simpredis/utils/logger"
	"sync"
	"syscall"
	"time"
)

type Handler interface {
	Handle(conn net.Conn)
	Close()
}

// handler is the application server
func ListenAndServe(cfg *config.Config, handler Handler) error {
	errCh := make(chan error, 1)
	closeCh := make(chan os.Signal, 1)
	signal.Notify(closeCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)

	addr := cfg.Bind + ":" + cfg.Port
	listener, err := net.Listen("tcp4", addr)
	if err != nil {
		logger.Error("Fail to start listen: %v", err)
		return err
	}
	logger.Info("Server start listening at %s", addr)

	// 优雅关闭
	go func() {
		select {
		case s := <-closeCh:
			logger.Error("Get signal:%v", s)
		case err := <-errCh:
			logger.Error("Get link error: %v", err)
		}
		listener.Close()
		logger.Info("Server stop Listening")
		handler.Close()
		logger.Info("Stopping simpredis server")
	}()

	var wg sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				logger.Warn("Accept occurs temporary error: %v, retry in 5ms", err)
				time.Sleep(5 * time.Millisecond)
				continue
			}
			errCh <- err
			break
		}
		logger.Info("Accept link from %s", conn.RemoteAddr().String())
		wg.Add(1)
		go func() {
			defer wg.Done()
			handler.Handle(conn)
		}()
	}

	waitingCh := make(chan struct{})
	go func() {
		wg.Wait()
		waitingCh <- struct{}{}
	}()
	// 等待所有连接关闭，如果数据库运行出现死锁这里会阻塞
	// 设置超时时间，若超时则自动关闭server
	select {
	case <-waitingCh:
		logger.Info("All connections are closed")
	case <-time.After(10 * time.Second):
		logger.Info("Timeout: shutting down server")
	}
	return nil
}
