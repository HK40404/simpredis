package database

import (
	"testing"

	parser "github.com/HK40404/simpredis/redis/resp"
	. "github.com/HK40404/simpredis/utils/client"
)

func TestCon(t *testing.T) {
	engine := NewDBEngine()

	args := LineToArgs("ping")
	reply := engine.ExecCmd(args)
	if reply.(*parser.String).Arg != "PONG" {
		t.Log(reply.(*parser.String).Arg)
		t.Fail()
	}

	args = LineToArgs("ping 123")
	reply = engine.ExecCmd(args)
	data := reply.(*parser.BulkString).Arg
	if string(data) != "123" {
		t.Fail()
	}

	args = LineToArgs("echo 123")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if string(data) != "123" {
		t.Fail()
	}
}
