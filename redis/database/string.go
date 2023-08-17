package database

import (
	"math"
	parser "simpredis/redis/resp"
	"strconv"
	"strings"
	"time"
)

const (
	SETNON = iota
	SETNX
	SETXX
)

// 如果执行失败，就是命令语法有问题
func ExecSet(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) < 3 {
		return parser.NewError("Invalid command format")
	}
	
	key := string(args[1])
	value := string(args[2])
	delayTime := time.Duration(0)
	setFlag := SETNON

	for i := 3; i < len(args); i++ {
		arg := strings.ToLower(string(args[i]))
		switch arg {
		case "nx":
			setFlag = SETNX
		case "xx":
			setFlag = SETXX
		case "ex":
			if i+1 >= len(args) {
				return parser.NewError("Invalid command format")
			}
			i++
			seconds, err := strconv.Atoi(string(args[i]))
			if err != nil {
				return parser.NewError("Invalid command format")
			}
			if seconds < 0 {
				// 时间不能为负数
				return parser.NewError("Invalid command format")
			}
			delayTime = time.Duration(seconds)*time.Second
		case "px":
			if i+1 >= len(args) {
				return parser.NewError("Invalid command format")
			}
			i++
			milliseconds, err := strconv.Atoi(string(args[i]))
			if err != nil {
				return parser.NewError("Invalid command format")
			}
			if milliseconds < 0 {
				// 时间不能为负数
				return parser.NewError("Invalid command format")
			}
			delayTime = time.Duration(milliseconds)*time.Millisecond
		default:
			// 不支持的参数
			return parser.NewError("Invalid command format")
		}
	}

	switch setFlag {
	case SETNON:
		engine.lock.Lock(key)
		defer engine.lock.UnLock(key)
		engine.db.SetWithLock(key, value)
		if delayTime == time.Duration(0) {
			engine.DelTTL(key)
		} else {
			engine.SetTTL(key, delayTime)
		}
		return parser.MakeOKReply()
	case SETNX:
		engine.lock.Lock(key)
		defer engine.lock.UnLock(key)
		if _, ok := engine.db.GetWithLock(key); ok {
			return parser.MakeNullBulkReply()
		}
		engine.db.SetWithLock(key, value)
		if delayTime == time.Duration(0) {
			engine.DelTTL(key)
		} else {
			engine.SetTTL(key, delayTime)
		}
		return parser.MakeOKReply()
	case SETXX:
		engine.lock.Lock(key)
		defer engine.lock.UnLock(key)
		if _, ok := engine.db.GetWithLock(key); !ok {
			return parser.MakeNullBulkReply()
		}
		engine.db.SetWithLock(key, value)
		if delayTime == time.Duration(0) {
			engine.DelTTL(key)
		} else {
			engine.SetTTL(key, delayTime)
		}
		return parser.MakeOKReply()
	}

	return parser.NewError("Unknow error")
}

func ExecGet(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 2 {
		return parser.NewError("Invalid command format")
	}
	key := string(args[1])

	engine.lock.RLock(key)
	defer engine.lock.RUnLock(key)
	v, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.MakeNullBulkReply()
	}
	str, ok := v.(string)
	if !ok {
		return parser.NewError("Operation against a key holding the wrong kind of value")
	}
	return parser.NewBulkString([]byte(str))
}

func ExecIncr(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 2 {
		return parser.NewError("Invalid command format")
	}
	
	key := string(args[1])
	
	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)
	v, ok := engine.db.GetWithLock(key)
	// item不存在的情况，初始化为0然后自增
	if !ok {
		engine.db.SetWithLock(key, "1")
		return parser.NewInteger(1)
	}
	s, ok := v.(string)
	if !ok {
		return parser.NewError("Value is not an integer")
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return parser.NewError("Value is not an integer")
	}
	if n == math.MaxInt64  {
		return parser.NewError("Value is out of range")
	}
	n++
	engine.db.SetWithLock(key, strconv.Itoa(n))
	return parser.NewInteger(int64(n))
}

func ExecMset(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) < 3 || len(args) % 2 != 1 {
		return parser.NewError("Invalid command format")
	}
	keys := make([]string, 0, (len(args)-1)/2)
	values := make([]string, 0, (len(args)-1)/2)
	for i := 1; i < len(args); i+=2 {
		keys = append(keys, string(args[i]))
		values = append(values, string(args[i+1]))
	}
	engine.lock.Locks(keys)
	defer engine.lock.UnLocks(keys)
	
	for i := 0; i < len(keys); i++ {
		engine.db.SetWithLock(keys[i], values[i])
	}
	return parser.MakeOKReply()
}

func ExecMget(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) < 2 {
		return parser.NewError("Invalid command format")
	}
	keys := make([]string, 0, len(args)-1)
	for i := 1; i < len(args); i++ {
		keys = append(keys, string(args[i]))
	}
	engine.lock.RLocks(keys)
	defer engine.lock.RUnLocks(keys)
	
	values := make([][]byte, 0, len(keys))
	for i := 0; i < len(keys); i++ {
		item, ok := engine.db.GetWithLock(keys[i])
		if !ok {
			values = append(values, nil)
			continue
		}
		v, ok := item.(string)
		if !ok {
			values = append(values, nil)
			continue
		}
		values = append(values, []byte(v))
	}
	return parser.NewArray(values)
}

func init() {
	RegisterCmd("set", ExecSet)
	RegisterCmd("get", ExecGet)
	RegisterCmd("mset", ExecMset)
	RegisterCmd("mget", ExecMget)
	RegisterCmd("incr", ExecIncr)
}