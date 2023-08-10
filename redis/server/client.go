package handler

import (
	"net"
	"sync"
	"time"
)

type Client struct {
	Conn net.Conn
	Wg sync.WaitGroup
}

func NewClient(con net.Conn) *Client {
	return &Client{
		Conn: con,
	}
}

// 给还在传输数据的连接一些时间处理
func (c *Client) Close() {
	ch := make(chan struct{})
	go func ()  {
		c.Wg.Wait()
		ch <- struct{}{}	
	}()
	select {
	case <-ch:
		break
	case <-time.After(3 * time.Second):
		break
	}
	c.Conn.Close()
}