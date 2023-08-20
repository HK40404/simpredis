package database

import (
	parser "simpredis/redis/resp"
	. "simpredis/utils/client"
	"strconv"
	"testing"
)

func TestLpsuh(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("lpush l 3 2 1 ")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 3 {
		t.Fail()
	}

	args = LineToArgs("set k v")
	engine.ExecCmd(args)
	args = LineToArgs("lpush k 1 2 3 ")
	reply = engine.ExecCmd(args)
	if _, ok := reply.(*parser.Error); !ok {
		t.Fail()
	}

	args = LineToArgs("lrange l 0 -1")
	reply = engine.ExecCmd(args)
	data := reply.(*parser.Array).Args
	for i := 0; i < len(data); i++ {
		if string(data[i]) != strconv.Itoa(i+1) {
			t.Fail()
		}
	}
	if t.Failed() {
		t.Logf("lrange: %s", data)
	}
}

func TestLpop(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("lpush l 3 2 1 ")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 3 {
		t.Fail()
	}

	args = LineToArgs("set k v")
	engine.ExecCmd(args)
	args = LineToArgs("lpop k")
	reply = engine.ExecCmd(args)
	if _, ok := reply.(*parser.Error); !ok {
		t.Fail()
	}

	args = LineToArgs("lpop l")
	reply = engine.ExecCmd(args)
	data := reply.(*parser.BulkString).Arg
	if string(data) != "1" {
		t.Fail()
	}
}

func TestRpush(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("rpush l 1 2 3 ")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 3 {
		t.Fail()
	}

	args = LineToArgs("set k v")
	engine.ExecCmd(args)
	args = LineToArgs("rpush k 1 2 3")
	reply = engine.ExecCmd(args)
	if _, ok := reply.(*parser.Error); !ok {
		t.Fail()
	}

	args = LineToArgs("lrange l 0 -1")
	reply = engine.ExecCmd(args)
	data := reply.(*parser.Array).Args
	for i := 0; i < len(data); i++ {
		if string(data[i]) != strconv.Itoa(i+1) {
			t.Fail()
		}
	}
	if t.Failed() {
		t.Logf("lrange: %s", data)
	}
}

func TestRpop(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("lpush l 3 2 1 ")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 3 {
		t.Fail()
	}

	args = LineToArgs("set k v")
	engine.ExecCmd(args)
	args = LineToArgs("rpop k")
	reply = engine.ExecCmd(args)
	if _, ok := reply.(*parser.Error); !ok {
		t.Fail()
	}

	args = LineToArgs("rpop l")
	reply = engine.ExecCmd(args)
	data := reply.(*parser.BulkString).Arg
	if string(data) != "3" {
		t.Fail()
	}
}

func TestLindex(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("rpush l 1 2 3 4 5 6 ")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 6 {
		t.Fail()
	}

	args = LineToArgs("set k v")
	engine.ExecCmd(args)
	args = LineToArgs("lindex k 0")
	reply = engine.ExecCmd(args)
	if _, ok := reply.(*parser.Error); !ok {
		t.Fail()
	}

	reply = engine.ExecCmd(LineToArgs("lindex l 0"))
	data := reply.(*parser.BulkString).Arg
	if string(data) != "1" {
		t.Fail()
	}

	reply = engine.ExecCmd(LineToArgs("lindex l -1"))
	data = reply.(*parser.BulkString).Arg
	if string(data) != "6" {
		t.Fail()
	}

	reply = engine.ExecCmd(LineToArgs("lindex l -4"))
	data = reply.(*parser.BulkString).Arg
	if string(data) != "3" {
		t.Fail()
	}

	reply = engine.ExecCmd(LineToArgs("lindex l 6"))
	data = reply.(*parser.BulkString).Arg
	if data != nil {
		t.Fail()
	}

	reply = engine.ExecCmd(LineToArgs("lindex l1 0"))
	data = reply.(*parser.BulkString).Arg
	if data != nil {
		t.Fail()
	}

	reply = engine.ExecCmd(LineToArgs("lindex l -7"))
	data = reply.(*parser.BulkString).Arg
	if data != nil {
		t.Fail()
	}
}

func TestLrange(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("rpush l 1 2 3 4 5 6 ")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 6 {
		t.Fail()
	}

	args = LineToArgs("set k v")
	engine.ExecCmd(args)
	args = LineToArgs("lrange k 0 -1")
	reply = engine.ExecCmd(args)
	if _, ok := reply.(*parser.Error); !ok {
		t.Fail()
	}

	args = LineToArgs("lrange l 0 -3")
	reply = engine.ExecCmd(args)
	data := reply.(*parser.Array).Args
	for i := 0; i < 4; i++ {
		if string(data[i]) != strconv.Itoa(i+1) {
			t.Fail()
		}
	}

	args = LineToArgs("lrange l 4 100")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.Array).Args
	for i := 0; i < 2; i++ {
		if string(data[i]) != strconv.Itoa(i+5) {
			t.Fail()
		}
	}

	args = LineToArgs("lrange l 4 -6")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.Array).Args
	if data != nil {
		t.Fail()
	}
}

func TestLlen(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("rpush l 1 2 3 4 5 6 ")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 6 {
		t.Fail()
	}

	args = LineToArgs("set k v")
	engine.ExecCmd(args)
	args = LineToArgs("llen k 0 -1")
	reply = engine.ExecCmd(args)
	if _, ok := reply.(*parser.Error); !ok {
		t.Fail()
	}

	args = LineToArgs("llen l")
	reply = engine.ExecCmd(args)
	size := reply.(*parser.Integer).Arg
	if size != 6 {
		t.Fail()
	}

	args = LineToArgs("llen l1")
	reply = engine.ExecCmd(args)
	size = reply.(*parser.Integer).Arg
	if size != 0 {
		t.Fail()
	}
}

func TestLset(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("rpush l 1 2 3 4 5 6 ")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 6 {
		t.Fail()
	}

	args = LineToArgs("lset l 100 -1")
	reply = engine.ExecCmd(args)
	if _, ok := reply.(*parser.Error); !ok {
		t.Fail()
	}

	args = LineToArgs("lset l 0 play")
	engine.ExecCmd(args)
	args = LineToArgs("lindex l 0")
	reply = engine.ExecCmd(args)
	if string(reply.(*parser.BulkString).Arg) != "play" {
		t.Fail()
	}
}

func TestLpushX(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("lpushx l 1")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Fail()
	}

	args = LineToArgs("rpush l 1 2 3 4 5 6 ")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 6 {
		t.Fail()
	}

	args = LineToArgs("lpushx l 100")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 7 {
		t.Fail()
	}
	args = LineToArgs("lindex l 0")
	reply = engine.ExecCmd(args)
	if string(reply.(*parser.BulkString).Arg) != "100" {
		t.Fail()
	}
}

func TestRpushX(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("rpushx l 1")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Fail()
	}

	args = LineToArgs("rpush l 1 2 3 4 5 6 ")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 6 {
		t.Fail()
	}

	args = LineToArgs("rpushx l 100")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 7 {
		t.Fail()
	}
	args = LineToArgs("lindex l -1")
	reply = engine.ExecCmd(args)
	if string(reply.(*parser.BulkString).Arg) != "100" {
		t.Fail()
	}
}

func TestRpopLpush(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("rpush src 1 2 3 4 5 6 ")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 6 {
		t.Fail()
	}

	args = LineToArgs("rpush dst 0 ")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("rpoplpush src dst")
	reply = engine.ExecCmd(args)
	if string(reply.(*parser.BulkString).Arg) != "6" {
		t.Fail()
	}
	args = LineToArgs("lindex dst 0")
	reply = engine.ExecCmd(args)
	if string(reply.(*parser.BulkString).Arg) != "6" {
		t.Fail()
	}

	args = LineToArgs("rpoplpush src dst2")
	reply = engine.ExecCmd(args)
	if string(reply.(*parser.BulkString).Arg) != "5" {
		t.Fail()
	}
	args = LineToArgs("lindex dst2 0")
	reply = engine.ExecCmd(args)
	if string(reply.(*parser.BulkString).Arg) != "5" {
		t.Fail()
	}
}

func TestLinsert(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("rpush src 1 2 3 4 5 6 ")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 6 {
		t.Fail()
	}

	args = LineToArgs("linsert dst before 0 1")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Fail()
	}

	args = LineToArgs("linsert src before 0 1")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != -1 {
		t.Fail()
	}

	// 1 happy 2 3 4 5 6
	args = LineToArgs("linsert src before 2 happy")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 7 {
		t.Fail()
	}
	args = LineToArgs("lindex src 1")
	reply = engine.ExecCmd(args)
	if string(reply.(*parser.BulkString).Arg) != "happy" {
		t.Fail()
	}

	// 1 happy 2 3 4 life 5 6
	args = LineToArgs("linsert src after 4 life")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 8 {
		t.Fail()
	}
	args = LineToArgs("lindex src 5")
	reply = engine.ExecCmd(args)
	if string(reply.(*parser.BulkString).Arg) != "life" {
		t.Fail()
	}
}

func TestLrem(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("rpush l 1 1 1 1 2 2 3 2 3 3")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 10 {
		t.Fail()
	}

	args = LineToArgs("lrem noexist 100 1")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Fail()
	}

	// 1 2 2 3 2 3 3
	args = LineToArgs("lrem l 3 1")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 3 {
		t.Fail()
	}
	args = LineToArgs("lindex l 0")
	reply = engine.ExecCmd(args)
	if string(reply.(*parser.BulkString).Arg) != "1" {
		t.Fail()
	}

	// 1 2 2 3 2
	args = LineToArgs("lrem l -2 3")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 2 {
		t.Fail()
	}
	args = LineToArgs("lindex l -2")
	reply = engine.ExecCmd(args)
	if string(reply.(*parser.BulkString).Arg) != "3" {
		t.Fail()
	}

	// 1 3
	args = LineToArgs("lrem l 0 2")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 3 {
		t.Fail()
	}
	args = LineToArgs("lindex l -1")
	reply = engine.ExecCmd(args)
	if string(reply.(*parser.BulkString).Arg) != "3" {
		t.Fail()
	}
}

func TestTrim(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("rpush l 1 2 3 4 5 6 7 8 9")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 9 {
		t.Fail()
	}

	args = LineToArgs("ltrim l 1 1000")
	engine.ExecCmd(args)
	args = LineToArgs("lindex l 0")
	reply = engine.ExecCmd(args)
	if string(reply.(*parser.BulkString).Arg) != "2" {
		t.Fail()
	}

	args = LineToArgs("ltrim l -1 1")
	engine.ExecCmd(args)
	args = LineToArgs("lindex l 0")
	reply = engine.ExecCmd(args)
	if reply.(*parser.BulkString).Arg != nil {
		t.Fail()
	}
}