package database

import (
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
func ExecSet(engine *DBEngine, array [][]byte) parser.RespData {
	if len(array) < 3 {
		return parser.NewError("Invalid command format")
	}
	
	key := array[1]
	value := array[2]
	var expireTime time.Time
	setFlag := SETNON
	engine.CheckTTL(string(key))

	for i := 3; i < len(array); i++ {
		arg := strings.ToLower(string(array[i]))
		switch arg {
		case "nx":
			setFlag = SETNX
		case "xx":
			setFlag = SETXX
		case "ex":
			if i+1 >= len(array) {
				return parser.NewError("Invalid command format")
			}
			i++
			seconds, err := strconv.Atoi(string(array[i]))
			if err != nil {
				return parser.NewError("Invalid command format")
			}
			if seconds < 0 {
				// 时间不能为负数
				return parser.NewError("Invalid command format")
			}
			expireTime = time.Now().Add(time.Duration(seconds)*time.Second)
		case "px":
			if i+1 >= len(array) {
				return parser.NewError("Invalid command format")
			}
			i++
			milliseconds, err := strconv.Atoi(string(array[i]))
			if err != nil {
				return parser.NewError("Invalid command format")
			}
			if milliseconds < 0 {
				// 时间不能为负数
				return parser.NewError("Invalid command format")
			}
			expireTime = time.Now().Add(time.Duration(milliseconds)*time.Millisecond)
		default:
			// 不支持的参数
			return parser.NewError("Invalid command format")
		}
	}

	switch setFlag {
	case SETNON:
		engine.lock.Lock(string(key))
		defer engine.lock.UnLock(string(key))
		engine.db.SetWithLock(string(key), string(value))
		if expireTime.IsZero() {
			engine.DelTTL(string(key))
		} else {
			engine.SetTTL(string(key), expireTime.Unix())
		}
		return parser.MakeOKReply()
	case SETNX:
		engine.lock.Lock(string(key))
		defer engine.lock.UnLock(string(key))
		if _, ok := engine.db.GetWithLock(string(key)); ok {
			return parser.MakeNullBulkReply()
		}
		engine.db.SetWithLock(string(key), string(value))
		if expireTime.IsZero() {
			engine.DelTTL(string(key))
		} else {
			engine.SetTTL(string(key), expireTime.Unix())
		}
		return parser.MakeOKReply()
	case SETXX:
		engine.lock.Lock(string(key))
		defer engine.lock.UnLock(string(key))
		if _, ok := engine.db.GetWithLock(string(key)); !ok {
			return parser.MakeNullBulkReply()
		}
		engine.db.SetWithLock(string(key), string(value))
		if expireTime.IsZero() {
			engine.DelTTL(string(key))
		} else {
			engine.SetTTL(string(key), expireTime.Unix())
		}
		return parser.MakeOKReply()
	}

	return parser.NewError("Unknow error")
}

func ExecGet(engine *DBEngine, array [][]byte) parser.RespData {
	if len(array) != 2 {
		return parser.NewError("Invalid command format")
	}
	key := array[1]
	engine.CheckTTL(string(key))

	engine.lock.RLock(string(key))
	defer engine.lock.RUnLock(string(key))
	v, ok := engine.db.GetWithLock(string(key))
	if !ok {
		return parser.MakeNullBulkReply()
	}
	str, ok := v.(string)
	if !ok {
		return parser.NewError("Operation against a key holding the wrong kind of value")
	}
	return parser.NewString(str)
}

func init() {
	RegisterCmd("set", ExecSet)
	RegisterCmd("get", ExecGet)
}