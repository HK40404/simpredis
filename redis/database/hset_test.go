package database

import (
	"testing"

	parser "github.com/HK40404/simpredis/redis/resp"
	. "github.com/HK40404/simpredis/utils/client"
)

func TestHsetnxAndHgetAndHlen(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("hset h dog bark")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("hset h dog wouw")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Fail()
	}

	args = LineToArgs("hget h dog")
	reply = engine.ExecCmd(args)
	data := reply.(*parser.BulkString).Arg
	if string(data) != "wouw" {
		t.Fail()
	}

	args = LineToArgs("hget h cat")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if data != nil {
		t.Fail()
	}

	args = LineToArgs("hsetnx h dog meow")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Fail()
	}

	args = LineToArgs("hget h dog")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if string(data) != "wouw" {
		t.Fail()
	}

	args = LineToArgs("hsetnx h cat meow")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("hget h cat")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if string(data) != "meow" {
		t.Fail()
	}

	args = LineToArgs("hlen h")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 2 {
		t.Fail()
	}

	args = LineToArgs("hlen noexist")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Fail()
	}
}

func TestHdelAndHexist(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("hset h dog bark")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("hexists h cat")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Fail()
	}

	args = LineToArgs("hexists h dog")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("hset h cat meow")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("hexists h cat")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("hdel h cat dog")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 2 {
		t.Fail()
	}

	args = LineToArgs("hexists h cat")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Fail()
	}

	args = LineToArgs("hdel h cat dog")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Fail()
	}
}

func TestHkeyvalsAndHgetall(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("hset h dog bark")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("hset h cat meow")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("hset h duck quack")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("hgetall no_exists")
	reply = engine.ExecCmd(args)
	data := reply.(*parser.Array).Args
	if data != nil {
		t.Fail()
	}

	args = LineToArgs("hgetall h")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.Array).Args
	items := make(map[string]string)
	for i := 0; i < len(data); i += 2 {
		items[string(data[i])] = string(data[i+1])
	}

	args = LineToArgs("hkeys no_exists")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.Array).Args
	if data != nil {
		t.Fail()
	}

	args = LineToArgs("hkeys h")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.Array).Args
	keys := make(map[string]struct{})
	for i := 0; i < len(data); i++ {
		keys[string(data[i])] = struct{}{}
	}

	args = LineToArgs("hvals no_exists")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.Array).Args
	if data != nil {
		t.Fail()
	}

	args = LineToArgs("hvals h")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.Array).Args
	vals := make(map[string]struct{})
	for i := 0; i < len(data); i++ {
		vals[string(data[i])] = struct{}{}
	}

	for k, v := range items {
		if _, ok := keys[k]; !ok {
			t.Fail()
		}
		delete(keys, k)
		if _, ok := vals[v]; !ok {
			t.Fail()
		}
		delete(vals, v)
	}
}

func TestHincrbyAndHincrbyfloat(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("hset h dog bark")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("hincrby h dog 100")
	reply = engine.ExecCmd(args)
	if _, ok := reply.(*parser.Error); !ok {
		t.Fail()
	}

	args = LineToArgs("hincrby h num1 100")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 100 {
		t.Fail()
	}

	args = LineToArgs("hincrby h num1 -1000")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != -900 {
		t.Fail()
	}

	args = LineToArgs("hincrbyfloat h num1 1e3")
	reply = engine.ExecCmd(args)
	data := reply.(*parser.BulkString).Arg
	if string(data) != "100" {
		t.Fail()
	}

	args = LineToArgs("hincrbyfloat h num2 3.14")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if string(data) != "3.14" {
		t.Fail()
	}

	args = LineToArgs("hincrbyfloat h dog 3.14")
	reply = engine.ExecCmd(args)
	if _, ok := reply.(*parser.Error); !ok {
		t.Fail()
	}

	args = LineToArgs("hset h num3 3.14e2")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("hincrbyfloat h num3 0.314")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if string(data) != "314.314" {
		t.Fail()
	}
}

func TestHmsetAndHmget(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("hmset h dog wouw cat meow duck quack")
	reply := engine.ExecCmd(args)
	if reply.(*parser.String).Arg != parser.MakeOKReply().Arg {
		t.Fail()
	}

	args = LineToArgs("set k v")
	engine.ExecCmd(args)
	args = LineToArgs("hmset k dog wouw cat meow duck quack")
	reply = engine.ExecCmd(args)
	if _, ok := reply.(*parser.Error); !ok {
		t.Fail()
	}

	args = LineToArgs("hmget h dog cat duck cow")
	reply = engine.ExecCmd(args)
	data := reply.(*parser.Array).Args
	if string(data[0]) != "wouw" {
		t.Fail()
	}
	if string(data[1]) != "meow" {
		t.Fail()
	}
	if string(data[2]) != "quack" {
		t.Fail()
	}
	if data[3] != nil {
		t.Fail()
	}
}
