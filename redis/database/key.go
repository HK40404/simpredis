package database

import (
	"strconv"
	"time"

	parser "github.com/HK40404/simpredis/redis/resp"
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

	engine.SetTTL(key, time.Duration(seconds)*time.Second)
	return parser.NewInteger(1)
}

func ExecExpireat(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 3 {
		return parser.NewError("Invalid command format")
	}
	key := string(args[1])
	timestamp, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return parser.NewError("Value is not an integer or out of range")
	}

	if timestamp < 0 {
		return parser.NewInteger(0)
	}

	engine.lock.RLock(key)
	defer engine.lock.RUnLock(key)
	_, ok := engine.db.GetWithLock(key)
	if !ok {
		// 不存在key，执行失败
		return parser.NewInteger(0)
	}

	// 直接过期
	if timestamp <= time.Now().Unix() {
		engine.db.DelWithLock(key)
		return parser.NewInteger(1)
	}

	engine.SetTTL(key, time.Until(time.Unix(timestamp, 0)))
	return parser.NewInteger(1)
}

func ExecDel(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) < 2 {
		return parser.NewError("Invalid command format")
	}

	keys := make([]string, 0, len(args[1:]))
	for i := 1; i < len(args); i++ {
		keys = append(keys, string(args[i]))
	}

	delCount := 0
	for _, k := range keys {
		engine.lock.Lock(k)
		if engine.db.DelWithLock(k) {
			delCount++
		}
		engine.CancelTTL(k)
		engine.lock.UnLock(k)
	}
	return parser.NewInteger(int64(delCount))
}

func ExecExists(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 2 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	engine.lock.RLock(key)
	defer engine.lock.RUnLock(key)
	_, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.NewInteger(0)
	}
	return parser.NewInteger(1)
}

func ExecPersist(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 2 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	engine.lock.RLock(key)
	defer engine.lock.RUnLock(key)
	_, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.NewInteger(0)
	}
	if !engine.CancelTTL(key) {
		return parser.NewInteger(0)
	}

	return parser.NewInteger(1)
}

func ExecRename(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 3 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	newkey := string(args[2])

	engine.lock.Locks([]string{key, newkey})
	defer engine.lock.UnLocks([]string{key, newkey})

	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.NewError("No such key")
	}

	engine.db.DelWithLock(newkey)
	engine.CancelTTL(newkey)

	var interval time.Duration
	t, ok := engine.ttldb.Get(key)
	if ok {
		interval = time.Until(time.Unix(t.(int64), 0))
	}

	engine.SetTTL(newkey, interval)
	engine.db.SetWithLock(newkey, item)
	engine.db.DelWithLock(key)
	engine.CancelTTL(key)
	return parser.MakeOKReply()
}

func ExecRenamenx(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 3 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	newkey := string(args[2])

	engine.lock.Locks([]string{key, newkey})
	defer engine.lock.UnLocks([]string{key, newkey})

	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.NewError("No such key")
	}

	_, ok = engine.db.GetWithLock(newkey)
	if ok {
		return parser.NewInteger(0)
	}

	var interval time.Duration
	t, ok := engine.ttldb.Get(key)
	if ok {
		interval = time.Until(time.Unix(t.(int64), 0))
	}

	engine.SetTTL(newkey, interval)
	engine.db.SetWithLock(newkey, item)
	engine.db.DelWithLock(key)
	engine.CancelTTL(key)
	return parser.NewInteger(1)
}

func ExecType(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 2 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])

	engine.lock.RLock(key)
	defer engine.lock.RUnLock(key)

	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.NewString("none")
	}

	switch item.(type) {
	case []byte:
		return parser.NewString("string")
	case *QuickList:
		return parser.NewString("list")
	case *Set:
		return parser.NewString("set")
	case *HashTable:
		return parser.NewString("hash")
	default:
		return parser.NewString("unknow type")
	}
}

func init() {
	RegisterCmd("ttl", ExecTTL)
	RegisterCmd("expire", ExecExpire)
	RegisterCmd("expireat", ExecExpireat)
	RegisterCmd("persist", ExecPersist)
	RegisterCmd("del", ExecDel)
	RegisterCmd("exists", ExecExists)
	RegisterCmd("rename", ExecRename)
	RegisterCmd("renamenx", ExecRenamenx)
	RegisterCmd("type", ExecType)
}
