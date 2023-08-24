package database

import (
	"bytes"
	"testing"
	"time"

	parser "github.com/HK40404/simpredis/redis/resp"
	. "github.com/HK40404/simpredis/utils/client"
)

func TestSetAndGet(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("set dog bark")
	reply := engine.ExecCmd(args)
	if reply.(*parser.String).Arg != "OK" {
		t.Fail()
	}

	args = LineToArgs("get dog")
	reply = engine.ExecCmd(args)
	data := reply.(*parser.BulkString).Arg
	if string(data) != "bark" {
		t.Fail()
	}

	args = LineToArgs("set dog wouw nx")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if data != nil {
		t.Fail()
	}

	args = LineToArgs("set dog wouw xx")
	reply = engine.ExecCmd(args)
	if reply.(*parser.String).Arg != "OK" {
		t.Fail()
	}

	args = LineToArgs("get dog")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if string(data) != "wouw" {
		t.Fail()
	}

	args = LineToArgs("get cat")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if data != nil {
		t.Fail()
	}

	args = LineToArgs("set cat meow nx ex 1")
	reply = engine.ExecCmd(args)
	if reply.(*parser.String).Arg != "OK" {
		t.Fail()
	}

	args = LineToArgs("get cat")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if string(data) != "meow" {
		t.Fail()
	}

	time.Sleep(time.Second)
	args = LineToArgs("get cat")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if data != nil {
		t.Fail()
	}

	args = LineToArgs("setex cat 1 meow")
	reply = engine.ExecCmd(args)
	if reply.(*parser.String).Arg != "OK" {
		t.Fail()
	}

	args = LineToArgs("get cat")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if string(data) != "meow" {
		t.Fail()
	}

	time.Sleep(time.Second)
	args = LineToArgs("get cat")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if data != nil {
		t.Fail()
	}

	args = LineToArgs("setnx cat meow")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("setnx dog meow")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Fail()
	}

	args = LineToArgs("getset duck quack")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if data != nil {
		t.Fail()
	}

	args = LineToArgs("getset duck hahaha")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if string(data) != "quack" {
		t.Fail()
	}
}

func TestMsetAndget(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("msetnx cat meow dog bark")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("mget dog cat duck")
	reply = engine.ExecCmd(args)
	data := reply.(*parser.Array).Args
	if string(data[0]) != "bark" {
		t.Fail()
	}
	if string(data[1]) != "meow" {
		t.Fail()
	}
	if data[2] != nil {
		t.Fail()
	}

	args = LineToArgs("msetnx dog wouw duck quack")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Fail()
	}

	args = LineToArgs("mget duck")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.Array).Args
	if data[0] != nil {
		t.Fail()
	}

	args = LineToArgs("mset dog wouw duck quack")
	reply = engine.ExecCmd(args)
	if string(reply.(*parser.String).Arg) != "OK" {
		t.Fail()
	}

	args = LineToArgs("mget dog duck")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.Array).Args
	if string(data[0]) != "wouw" {
		t.Fail()
	}
	if string(data[1]) != "quack" {
		t.Fail()
	}
}

func TestIncrAndDecr(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("set k v")
	engine.ExecCmd(args)
	args = LineToArgs("incr k")
	reply := engine.ExecCmd(args)
	if _, ok := reply.(*parser.Error); !ok {
		t.Fail()
	}

	args = LineToArgs("set n 100")
	engine.ExecCmd(args)
	args = LineToArgs("incr n")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 101 {
		t.Fail()
	}

	args = LineToArgs("incr n1")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("incrby n2 99")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 99 {
		t.Fail()
	}

	args = LineToArgs("incrby k 100")
	reply = engine.ExecCmd(args)
	if _, ok := reply.(*parser.Error); !ok {
		t.Fail()
	}

	args = LineToArgs("incrby n2 -99")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Fail()
	}

	args = LineToArgs("incrbyfloat k 1e1")
	reply = engine.ExecCmd(args)
	if _, ok := reply.(*parser.Error); !ok {
		t.Fail()
	}

	args = LineToArgs("incrbyfloat f 1.11")
	reply = engine.ExecCmd(args)
	if string(reply.(*parser.BulkString).Arg) != "1.11" {
		t.Fail()
	}

	args = LineToArgs("set f2 1e10")
	engine.ExecCmd(args)
	args = LineToArgs("incrbyfloat f2 -1e10")
	reply = engine.ExecCmd(args)
	if string(reply.(*parser.BulkString).Arg) != "0" {
		t.Fail()
	}

	args = LineToArgs("decr k")
	reply = engine.ExecCmd(args)
	if _, ok := reply.(*parser.Error); !ok {
		t.Fail()
	}

	args = LineToArgs("decr dn")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != -1 {
		t.Fail()
	}

	engine.ExecCmd(LineToArgs("set dn -99"))
	args = LineToArgs("decr dn")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != -100 {
		t.Fail()
	}

	args = LineToArgs("decrby k 100")
	reply = engine.ExecCmd(args)
	if _, ok := reply.(*parser.Error); !ok {
		t.Fail()
	}

	args = LineToArgs("decrby dn2 50")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != -50 {
		t.Fail()
	}

	args = LineToArgs("decrby dn2 500")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != -550 {
		t.Fail()
	}
}

func TestAppendAndStrlen(t *testing.T) {
	engine := NewDBEngine()
	engine.ExecCmd(LineToArgs("set k hello"))

	args := LineToArgs("strlen k")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 5 {
		t.Fail()
	}

	args = LineToArgs("strlen noexist")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Fail()
	}

	args = LineToArgs("append k ,world")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 11 {
		t.Fail()
	}
	args = LineToArgs("get k")
	reply = engine.ExecCmd(args)
	data := reply.(*parser.BulkString).Arg
	if string(data) != "hello,world" {
		t.Fail()
	}

	args = LineToArgs("append k1 !")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}
	args = LineToArgs("get k1")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if string(data) != "!" {
		t.Fail()
	}
}

func TestSetrangeAndGetrange(t *testing.T) {
	engine := NewDBEngine()
	engine.ExecCmd(LineToArgs("set k hello"))
	args := LineToArgs("setrange k 0 pa")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 5 {
		t.Fail()
	}
	args = LineToArgs("getrange k -100 123")
	reply = engine.ExecCmd(args)
	data := reply.(*parser.BulkString).Arg
	if string(data) != "pallo" {
		t.Fail()
	}
	args = LineToArgs("getrange k -4 -1")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if string(data) != "allo" {
		t.Fail()
	}

	args = LineToArgs("getrange noexist 0 -1")
	reply = engine.ExecCmd(args)
	if reply.(*parser.BulkString).Arg == nil {
		t.Fail()
	}
	if !bytes.Equal(reply.Serialize(), []byte{'$', '0', '\r', '\n', '\r', '\n'}) {
		t.Fail()
	}

	args = LineToArgs("setrange k1 3 test")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 7 {
		t.Fail()
	}

	args = LineToArgs("getrange k1 0 -1")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if !bytes.Equal(data, []byte{0, 0, 0, 't', 'e', 's', 't'}) {
		t.Fail()
	}
}

func TestBitOps(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("setbit k 15 1")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Fail()
	}

	args = LineToArgs("get k")
	reply = engine.ExecCmd(args)
	data := reply.(*parser.BulkString).Arg
	if !bytes.Equal(data, []byte{0, 0b10000000}) {
		t.Fail()
	}

	args = LineToArgs("getbit k 15")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	engine.ExecCmd(LineToArgs("set k1 0"))
	args = LineToArgs("setbit k1 5 0")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("get k1")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if !bytes.Equal(data, []byte{16}) {
		t.Fail()
	}

	args = LineToArgs("getbit k1 150")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Fail()
	}

	args = LineToArgs("getbit noexist 0")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Fail()
	}

	args = LineToArgs("bitcount noexist")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Fail()
	}

	engine.ExecCmd(LineToArgs("set k2 ?"))
	args = LineToArgs("bitcount k2")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 6 {
		t.Fail()
	}

	args = LineToArgs("setbit k2 10 1")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Fail()
	}

	args = LineToArgs("bitcount k2 -1 -1")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("setbit k2 5 0")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("bitcount k2 -100 0")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 5 {
		t.Fail()
	}

	// n1: b1001
	engine.ExecCmd(LineToArgs("setbit n1 0 1"))
	engine.ExecCmd(LineToArgs("setbit n1 3 1"))
	// n2: b1011
	engine.ExecCmd(LineToArgs("setbit n2 3 1"))
	engine.ExecCmd(LineToArgs("setbit n2 1 1"))
	engine.ExecCmd(LineToArgs("setbit n2 0 1"))

	args = LineToArgs("bitop and res n1 n2")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}
	args = LineToArgs("get res")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if !bytes.Equal(data, []byte{0b1001}) {
		t.Fail()
	}

	t.Log(engine.ExecCmd(LineToArgs("get n1")).(*parser.BulkString).Arg)
	t.Log(engine.ExecCmd(LineToArgs("get n2")).(*parser.BulkString).Arg)

	args = LineToArgs("bitop or res n1 n2")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}
	args = LineToArgs("get res")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if !bytes.Equal(data, []byte{0b1011}) {
		t.Log(data)
		t.Fail()
	}

	t.Log(engine.ExecCmd(LineToArgs("get n1")).(*parser.BulkString).Arg)
	t.Log(engine.ExecCmd(LineToArgs("get n2")).(*parser.BulkString).Arg)

	args = LineToArgs("bitop xor res n1 n2")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}
	args = LineToArgs("get res")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if !bytes.Equal(data, []byte{0b0010}) {
		t.Log(data)
		t.Fail()
	}

	args = LineToArgs("bitop not res n2")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}
	args = LineToArgs("get res")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if !bytes.Equal(data, []byte{0b11110100}) {
		t.Log(data)
		t.Fail()
	}

	// n3 : 0x 00 00 80
	// n1 : 0x 09 00 00
	// n2 : 0x 0b 00 00
	engine.ExecCmd(LineToArgs("setbit n3 23 1"))
	args = LineToArgs("bitop xor res n3 n2")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 3 {
		t.Fail()
	}
	args = LineToArgs("get res")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if !bytes.Equal(data, []byte{0x0b, 0, 0x80}) {
		t.Log(data)
		t.Fail()
	}

	args = LineToArgs("bitop not res n3 n2")
	reply = engine.ExecCmd(args)
	if _, ok := reply.(*parser.Error); !ok {
		t.Fail()
	}

	args = LineToArgs("bitop xor res noexist")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Fail()
	}
}
