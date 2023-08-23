package database

import (
	parser "simpredis/redis/resp"
	. "simpredis/utils/client"
	"strconv"
	"testing"
	"time"
)

func TestExistsAndDel(t *testing.T) {
	engine := NewDBEngine()
	engine.ExecCmd(LineToArgs("set k v"))

	args := LineToArgs("exists k")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("del k")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("exists k")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Fail()
	}

	engine.ExecCmd(LineToArgs("set k v"))
	engine.ExecCmd(LineToArgs("set k1 v"))
	engine.ExecCmd(LineToArgs("set k2 v"))
	engine.ExecCmd(LineToArgs("set k3 v"))

	args = LineToArgs("del k k1 k2 k3 k4")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 4 {
		t.Fail()
	}
}

// 允许1s以内的误差
func TestExpire(t *testing.T) {
	engine := NewDBEngine()
	engine.ExecCmd(LineToArgs("set k v"))

	args := LineToArgs("expire noexist 100")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Log(reply.(*parser.Integer).Arg)
		t.Fail()
	}

	args = LineToArgs("expire k 3")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Log(reply.(*parser.Integer).Arg)
		t.Fail()
	}

	args = LineToArgs("ttl k")
	reply = engine.ExecCmd(args)
	if !(reply.(*parser.Integer).Arg >= 2 && reply.(*parser.Integer).Arg <= 3) {
		t.Log(reply.(*parser.Integer).Arg)
		t.Fail()
	}

	time.Sleep(4*time.Second)

	args = LineToArgs("ttl k")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != -2 {
		t.Log(reply.(*parser.Integer).Arg)
		t.Fail()
	}

	engine.ExecCmd(LineToArgs("set k v"))
	args = LineToArgs("ttl k")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != -1 {
		t.Log(reply.(*parser.Integer).Arg)
		t.Fail()
	}

	ts := time.Now().Add(time.Second).Unix()
	args = LineToArgs("expireat k " + strconv.Itoa(int(ts)))
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Log(reply.(*parser.Integer).Arg)
		t.Fail()
	}

	time.Sleep(2*time.Second)
	args = LineToArgs("expireat k 123")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Log(reply.(*parser.Integer).Arg)
		t.Fail()
	}

	engine.ExecCmd(LineToArgs("set k v"))
	args = LineToArgs("expire k 3")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Log(reply.(*parser.Integer).Arg)
		t.Fail()
	}
	args = LineToArgs("ttl k")
	reply = engine.ExecCmd(args)
	if !(reply.(*parser.Integer).Arg >= 2 && reply.(*parser.Integer).Arg <= 3) {
		t.Log(reply.(*parser.Integer).Arg)
		t.Fail()
	}

	args = LineToArgs("persist k")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Log(reply.(*parser.Integer).Arg)
		t.Fail()
	}
	args = LineToArgs("persist noexist")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Log(reply.(*parser.Integer).Arg)
		t.Fail()
	}

	args = LineToArgs("ttl k")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != -1 {
		t.Log(reply.(*parser.Integer).Arg)
		t.Fail()
	}
}

func TestRename(t *testing.T) {
	engine := NewDBEngine()
	engine.ExecCmd(LineToArgs("set k v"))
	engine.ExecCmd(LineToArgs("set k1 v"))

	args := LineToArgs("expire k 100")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Log(reply.(*parser.Integer).Arg)
		t.Fail()
	}

	args = LineToArgs("rename k k1")
	reply = engine.ExecCmd(args)
	if reply.(*parser.String).Arg != "OK" {
		t.Fail()
	}
	args = LineToArgs("ttl k1")
	reply = engine.ExecCmd(args)
	if !(reply.(*parser.Integer).Arg >= 99 && reply.(*parser.Integer).Arg <= 100) {
		t.Log(reply.(*parser.Integer).Arg)
		t.Fail()
	}

	args = LineToArgs("rename k1 k2")
	reply = engine.ExecCmd(args)
	if reply.(*parser.String).Arg != "OK" {
		t.Fail()
	}
	args = LineToArgs("ttl k2")
	reply = engine.ExecCmd(args)
	if !(reply.(*parser.Integer).Arg >= 99 && reply.(*parser.Integer).Arg <= 100) {
		t.Log(reply.(*parser.Integer).Arg)
		t.Fail()
	}

	args = LineToArgs("rename noexist k2")
	reply = engine.ExecCmd(args)
	if _, ok := reply.(*parser.Error); !ok {
		t.Fail()
	}

	args = LineToArgs("renamenx k2 k")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Log(reply.(*parser.Integer).Arg)
		t.Fail()
	}
	args = LineToArgs("ttl k")
	reply = engine.ExecCmd(args)
	if !(reply.(*parser.Integer).Arg >= 98 && reply.(*parser.Integer).Arg <= 100) {
		t.Log(reply.(*parser.Integer).Arg)
		t.Fail()
	}

	engine.ExecCmd(LineToArgs("set k2 v"))
	args = LineToArgs("renamenx k k2")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Log(reply.(*parser.Integer).Arg)
		t.Fail()
	}
}

func TestType(t *testing.T) {
	engine := NewDBEngine()
	engine.ExecCmd(LineToArgs("set k v"))
	engine.ExecCmd(LineToArgs("sadd s 1 2 3"))
	engine.ExecCmd(LineToArgs("lpush l 1 2 3"))
	engine.ExecCmd(LineToArgs("hset h dog doggy"))

	args := LineToArgs("type k")
	reply := engine.ExecCmd(args)
	if reply.(*parser.String).Arg != "string" {
		t.Log(reply.(*parser.String).Arg)
		t.Fail()
	}

	args = LineToArgs("type l")
	reply = engine.ExecCmd(args)
	if reply.(*parser.String).Arg != "list" {
		t.Log(reply.(*parser.String).Arg)
		t.Fail()
	}

	args = LineToArgs("type h")
	reply = engine.ExecCmd(args)
	if reply.(*parser.String).Arg != "hash" {
		t.Log(reply.(*parser.String).Arg)
		t.Fail()
	}

	args = LineToArgs("type noexist")
	reply = engine.ExecCmd(args)
	if reply.(*parser.String).Arg != "none" {
		t.Log(reply.(*parser.String).Arg)
		t.Fail()
	}
}