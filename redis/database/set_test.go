package database

import (
	parser "simpredis/redis/resp"
	. "simpredis/utils/client"
	"strconv"
	"testing"
)

func TestSadd(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("sadd s 1 2 3")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 3 {
		t.Fail()
	}
	args = LineToArgs("sadd s 1 2 3 4")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("sdiff s")
	reply = engine.ExecCmd(args)
	data := reply.(*parser.Array).Args
	m := make(map[string]struct{})
	for i := 0; i < len(data); i++ {
		m[string(data[i])] = struct{}{}
	}
	for i := 1; i <= 4; i++ {
		_, ok := m[strconv.Itoa(i)]
		if !ok {
			t.Fail()
		}
	}

	args = LineToArgs("set k v")
	engine.ExecCmd(args)
	args = LineToArgs("sadd k 1 2 3 ")
	reply = engine.ExecCmd(args)
	if _, ok := reply.(*parser.Error); !ok {
		t.Fail()
	}
}

func TestScard(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("sadd s 1 2 3")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 3 {
		t.Fail()
	}

	args = LineToArgs("scard s")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 3 {
		t.Fail()
	}

	args = LineToArgs("scard unknow")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Fail()
	}
}

func TestSdiffAndSdiffstore(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("sadd s 1 2 3 4 5 asd")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 6 {
		t.Fail()
	}
	args = LineToArgs("sadd s1 3 4")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 2 {
		t.Fail()
	}
	args = LineToArgs("sadd s2 2")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("sdiff s s1 s2")
	reply = engine.ExecCmd(args)
	data := reply.(*parser.Array).Args
	m := make(map[string]struct{})
	for i := 0; i < len(data); i++ {
		m[string(data[i])] = struct{}{}
	}
	if _, ok := m["1"]; !ok {
		t.Fail()
	}
	if _, ok := m["5"]; !ok {
		t.Fail()
	}
	if _, ok := m["asd"]; !ok {
		t.Fail()
	}

	args = LineToArgs("sdiffstore s3 s s1")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 4 {
		t.Fail()
	}
	
	args = LineToArgs("sdiff s3 ")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.Array).Args
	m = make(map[string]struct{})
	for i := 0; i < len(data); i++ {
		m[string(data[i])] = struct{}{}
	}
	if _, ok := m["1"]; !ok {
		t.Fail()
	}
	if _, ok := m["2"]; !ok {
		t.Fail()
	}
	if _, ok := m["5"]; !ok {
		t.Fail()
	}
	if _, ok := m["asd"]; !ok {
		t.Fail()
	}
}

func TestSinterAndSinterstore(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("sadd s 1 2 3 4 5 asd")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 6 {
		t.Fail()
	}
	args = LineToArgs("sadd s1 1 3 4 5 6")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 5 {
		t.Fail()
	}
	args = LineToArgs("sadd s2 1 2 3 5 asd")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 5 {
		t.Fail()
	}

	args = LineToArgs("sinter s s1 s2")
	reply = engine.ExecCmd(args)
	data := reply.(*parser.Array).Args
	m := make(map[string]struct{})
	for i := 0; i < len(data); i++ {
		m[string(data[i])] = struct{}{}
	}
	if _, ok := m["1"]; !ok {
		t.Fail()
	}
	if _, ok := m["5"]; !ok {
		t.Fail()
	}
	if _, ok := m["3"]; !ok {
		t.Fail()
	}

	args = LineToArgs("sinterstore s3 s s2")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 5 {
		t.Fail()
	}
	
	args = LineToArgs("sinter s3 ")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.Array).Args
	m = make(map[string]struct{})
	for i := 0; i < len(data); i++ {
		m[string(data[i])] = struct{}{}
	}
	if _, ok := m["1"]; !ok {
		t.Fail()
	}
	if _, ok := m["2"]; !ok {
		t.Fail()
	}
	if _, ok := m["3"]; !ok {
		t.Fail()
	}
	if _, ok := m["5"]; !ok {
		t.Fail()
	}
	if _, ok := m["asd"]; !ok {
		t.Fail()
	}
}

func TestSunionAndSunionstore(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("sadd s 1 2 3 4 5 asd")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 6 {
		t.Fail()
	}
	args = LineToArgs("sadd s1 1 3 4 5 6")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 5 {
		t.Fail()
	}
	args = LineToArgs("sadd s2 1 2 3 5 asd")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 5 {
		t.Fail()
	}

	args = LineToArgs("sunion s s1 s2")
	reply = engine.ExecCmd(args)
	data := reply.(*parser.Array).Args
	m := make(map[string]struct{})
	for i := 0; i < len(data); i++ {
		m[string(data[i])] = struct{}{}
	}
	for k := range m {
		switch k {
		case "1", "2", "3", "4", "5", "6", "asd":
		default:
			t.Fail()
		}
	}

	args = LineToArgs("sunionstore s no_exist s")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 6 {
		t.Fail()
	}
	
	args = LineToArgs("sunion s")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.Array).Args
	m = make(map[string]struct{})
	for i := 0; i < len(data); i++ {
		m[string(data[i])] = struct{}{}
	}
	for k := range m {
		switch k {
		case "1", "2", "3", "4", "5", "asd":
		default:
			t.Fail()
		}
	}
}

func TestSismemberAndSmembers(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("sadd s 1 2 3")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 3 {
		t.Fail()
	}

	args = LineToArgs("sismember s 1")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("sismember s asd")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Fail()
	}

	args = LineToArgs("smembers no_exist")
	reply = engine.ExecCmd(args)
	data := reply.(*parser.Array).Args
	if data != nil {
		t.Fail()
	}
	
	args = LineToArgs("smembers s")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.Array).Args
	m := make(map[string]struct{})
	for i := 0; i < len(data); i++ {
		m[string(data[i])] = struct{}{}
	}
	if _, ok := m["1"]; !ok {
		t.Fail()
	}
	if _, ok := m["2"]; !ok {
		t.Fail()
	}
	if _, ok := m["3"]; !ok {
		t.Fail()
	}
}

func TestSmove(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("sadd s 1 2 3")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 3 {
		t.Fail()
	}

	args = LineToArgs("sadd s1 1")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("smove s s1 1")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("scard s1")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("smove s s2 asd")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 0 {
		t.Fail()
	}

	args = LineToArgs("smove s s3 2")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}
	
	args = LineToArgs("smembers s3")
	reply = engine.ExecCmd(args)
	data := reply.(*parser.Array).Args
	m := make(map[string]struct{})
	for i := 0; i < len(data); i++ {
		m[string(data[i])] = struct{}{}
	}
	if _, ok := m["2"]; !ok {
		t.Fail()
	}

	args = LineToArgs("lpush s 1 2 3 ")
	reply = engine.ExecCmd(args)
	if _, ok := reply.(*parser.Error); !ok {
		t.Fail()
	}

	args = LineToArgs("smove s s3 3")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 1 {
		t.Fail()
	}

	args = LineToArgs("lpush s 1 2 3 ")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 3 {
		t.Fail()
	}
}

func TestSpop(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("sadd s 1 2")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 2 {
		t.Fail()
	}

	args = LineToArgs("spop s")
	reply = engine.ExecCmd(args)
	data := reply.(*parser.BulkString).Arg
	if string(data) != "1" && string(data) != "2" {
		t.Fail()
	}

	args = LineToArgs("lpush s 1 2 3 ")
	reply = engine.ExecCmd(args)
	if _, ok := reply.(*parser.Error); !ok {
		t.Fail()
	}

	args = LineToArgs("spop s")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.BulkString).Arg
	if string(data) != "1" && string(data) != "2" {
		t.Fail()
	}

	args = LineToArgs("lpush s 1 2 3 ")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 3 {
		t.Fail()
	}
}

func TestSrem(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("sadd s 1 2 3 4 5 6 7 8 9 10")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 10 {
		t.Fail()
	}

	args = LineToArgs("srem s 1 2 3 4 5 a b c d e f")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 5 {
		t.Fail()
	}

	args = LineToArgs("lpush s 1 2 3 ")
	reply = engine.ExecCmd(args)
	if _, ok := reply.(*parser.Error); !ok {
		t.Fail()
	}

	args = LineToArgs("srem s 6 7 8 9 10")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 5 {
		t.Fail()
	}

	args = LineToArgs("lpush s 1 2 3 ")
	reply = engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 3 {
		t.Fail()
	}
}

func TestSrandmember(t *testing.T) {
	engine := NewDBEngine()
	args := LineToArgs("sadd s 1 2 3")
	reply := engine.ExecCmd(args)
	if reply.(*parser.Integer).Arg != 3 {
		t.Fail()
	}

	args = LineToArgs("srandmember s 100")
	reply = engine.ExecCmd(args)
	data := reply.(*parser.Array).Args
	m := make(map[string]struct{})
	for i := 0; i < len(data); i++ {
		m[string(data[i])] = struct{}{}
	}
	if _, ok := m["1"]; !ok {
		t.Fail()
	}
	if _, ok := m["2"]; !ok {
		t.Fail()
	}
	if _, ok := m["3"]; !ok {
		t.Fail()
	}

	args = LineToArgs("srandmember s 0")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.Array).Args
	if data != nil {
		t.Fail()
	}

	args = LineToArgs("srandmember s -100")
	reply = engine.ExecCmd(args)
	data = reply.(*parser.Array).Args
	for _, v := range data {
		switch string(v) {
		case "1", "2", "3":
		default:
			t.Fail()
		}
	}
}
