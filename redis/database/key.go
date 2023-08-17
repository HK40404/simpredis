package database

import (
	parser "simpredis/redis/resp"
	"time"
	"strconv"
)

func ExecTTL(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 2 {
		return parser.NewError("Invalid command format")
	}
	key := string(args[1])
	
	engine.lock.RLock(key)
	defer engine.lock.RUnLock(key)
	if _, ok := engine.db.GetWithLock(key); !ok {
		return parser.NewInteger(-2)
	}

	t, ok := engine.ttldb.Get(key)
	if !ok {
		return parser.NewInteger(-1)
	}
	interval := time.Until(time.Unix(t.(int64), 0))
	return parser.NewInteger(int64(interval.Seconds()))
}

// 设置不成功返回0，成功返回1
func ExecExpire(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 3 {
		return parser.NewError("Invalid command format")
	}
	key := string(args[1])
	seconds, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return parser.NewError("Value is not an integer or out of range")
	}

	engine.lock.RLock(key)	
	defer engine.lock.RUnLock(key)
	_, ok := engine.db.GetWithLock(key)
	if !ok {
		// 不存在key，执行失败
		return parser.NewInteger(0)
	}

	// 直接过期
	if seconds <= 0 {
		engine.db.DelWithLock(key)
		return parser.NewInteger(1)
	}

	engine.SetTTL(key, time.Duration(seconds) * time.Second)
	return parser.NewInteger(1)
}

func init() {
	RegisterCmd("ttl", ExecTTL)
	RegisterCmd("expire", ExecExpire)
}