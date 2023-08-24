package database

import (
	"strconv"

	parser "github.com/HK40404/simpredis/redis/resp"
)

func ExecHset(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 4 {
		return parser.NewError("Invalid command format")
	}
	key := string(args[1])
	field := string(args[2])
	value := string(args[3])

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)

	var hset *HashTable
	item, ok := engine.db.GetWithLock(key)
	if !ok {
		hset = NewHashTable()
		defer engine.db.SetWithLock(key, hset)
	} else {
		hset, ok = item.(*HashTable)
		if !ok {
			return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}

	isNew := hset.Set(field, value)
	if isNew {
		return parser.NewInteger(1)
	}
	return parser.NewInteger(0)
}

func ExecHget(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 3 {
		return parser.NewError("Invalid command format")
	}
	key := string(args[1])
	field := string(args[2])

	engine.lock.RLock(key)
	defer engine.lock.RUnLock(key)

	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.MakeNullBulkReply()
	}
	hset, ok := item.(*HashTable)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	v := hset.Get(field)
	if v == "" {
		return parser.MakeNullBulkReply()
	}
	return parser.NewBulkString([]byte(v))
}

func ExecHlen(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 2 {
		return parser.NewError("Invalid command format")
	}
	key := string(args[1])

	engine.lock.RLock(key)
	defer engine.lock.RUnLock(key)

	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.NewInteger(0)
	}
	hset, ok := item.(*HashTable)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return parser.NewInteger(int64(hset.Len()))
}

func ExecHkeys(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 2 {
		return parser.NewError("Invalid command format")
	}
	key := string(args[1])

	engine.lock.RLock(key)
	defer engine.lock.RUnLock(key)

	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.NewArray(nil)
	}
	hset, ok := item.(*HashTable)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	keys := make([][]byte, 0, hset.Len())
	for _, k := range hset.Keys() {
		keys = append(keys, []byte(k))
	}
	return parser.NewArray(keys)
}

func ExecHvals(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 2 {
		return parser.NewError("Invalid command format")
	}
	key := string(args[1])

	engine.lock.RLock(key)
	defer engine.lock.RUnLock(key)

	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.NewArray(nil)
	}
	hset, ok := item.(*HashTable)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	values := make([][]byte, 0, hset.Len())
	for _, v := range hset.Values() {
		values = append(values, []byte(v))
	}
	return parser.NewArray(values)
}

func ExecHgetall(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 2 {
		return parser.NewError("Invalid command format")
	}
	key := string(args[1])

	engine.lock.RLock(key)
	defer engine.lock.RUnLock(key)

	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.NewArray(nil)
	}
	hset, ok := item.(*HashTable)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	items := make([][]byte, 0, hset.Len()*2)
	for _, item := range hset.ALL() {
		items = append(items, []byte(item))
	}
	return parser.NewArray(items)
}

func ExecHmset(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) < 4 || len(args)%2 != 0 {
		return parser.NewError("Invalid command format")
	}
	key := string(args[1])

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)

	var hset *HashTable
	item, ok := engine.db.GetWithLock(key)
	if !ok {
		hset = NewHashTable()
		defer engine.db.SetWithLock(key, hset)
	} else {
		hset, ok = item.(*HashTable)
		if !ok {
			return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}

	for i := 2; i < len(args); i += 2 {
		field := string(args[i])
		value := string(args[i+1])
		hset.Set(field, value)
	}
	return parser.MakeOKReply()
}

func ExecHmget(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) < 3 {
		return parser.NewError("Invalid command format")
	}
	key := string(args[1])

	engine.lock.RLock(key)
	defer engine.lock.RUnLock(key)

	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.NewArray(nil)
	}
	hset, ok := item.(*HashTable)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	values := make([][]byte, 0, len(args[2:]))
	for _, field := range args[2:] {
		value := hset.Get(string(field))
		if value == "" {
			values = append(values, nil)
		} else {
			values = append(values, []byte(value))
		}
	}
	return parser.NewArray(values)
}

func ExecHexists(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 3 {
		return parser.NewError("Invalid command format")
	}
	key := string(args[1])
	filed := string(args[2])

	engine.lock.RLock(key)
	defer engine.lock.RUnLock(key)

	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.NewInteger(0)
	}
	hset, ok := item.(*HashTable)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if hset.Exist(filed) {
		return parser.NewInteger(1)
	}
	return parser.NewInteger(0)
}

func ExecHdel(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) < 3 {
		return parser.NewError("Invalid command format")
	}
	key := string(args[1])

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)

	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.NewInteger(0)
	}
	hset, ok := item.(*HashTable)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	delCount := 0
	for i := 2; i < len(args); i++ {
		if hset.Remove(string(args[i])) {
			delCount++
		}
	}
	return parser.NewInteger(int64(delCount))
}

func ExecHsetnx(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 4 {
		return parser.NewError("Invalid command format")
	}
	key := string(args[1])
	field := string(args[2])
	value := string(args[3])

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)

	var hset *HashTable
	item, ok := engine.db.GetWithLock(key)
	if !ok {
		hset = NewHashTable()
		defer engine.db.SetWithLock(key, hset)
	} else {
		hset, ok = item.(*HashTable)
		if !ok {
			return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}

	if hset.Exist(field) {
		return parser.NewInteger(0)
	}

	hset.Set(field, value)
	return parser.NewInteger(1)
}

func ExecHincrby(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 4 {
		return parser.NewError("Invalid command format")
	}
	key := string(args[1])
	field := string(args[2])
	inc, err := strconv.Atoi(string(args[3]))
	if err != nil {
		return parser.NewError("Value is not an integer or out of range")
	}

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)

	var hset *HashTable
	item, ok := engine.db.GetWithLock(key)
	if !ok {
		hset = NewHashTable()
		defer engine.db.SetWithLock(key, hset)
	} else {
		hset, ok = item.(*HashTable)
		if !ok {
			return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}

	v := hset.Get(field)
	if v == "" {
		// filed不存在，直接设置为inc
		hset.Set(field, strconv.Itoa(inc))
		return parser.NewInteger(int64(inc))
	}

	n, err := strconv.Atoi(v)
	if err != nil {
		return parser.NewError("Hash value is not an integer")
	}
	hset.Set(field, strconv.Itoa(inc+n))
	return parser.NewInteger(int64(inc + n))
}

func ExecHincrbyfloat(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 4 {
		return parser.NewError("Invalid command format")
	}
	key := string(args[1])
	field := string(args[2])
	inc, err := strconv.ParseFloat(string(args[3]), 64)
	if err != nil {
		return parser.NewError("Value is not a valid float")
	}

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)

	var hset *HashTable
	item, ok := engine.db.GetWithLock(key)
	if !ok {
		hset = NewHashTable()
		defer engine.db.SetWithLock(key, hset)
	} else {
		hset, ok = item.(*HashTable)
		if !ok {
			return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}

	v := hset.Get(field)
	if v == "" {
		// filed不存在，直接设置为inc
		v = strconv.FormatFloat(inc, 'f', -1, 64)
		hset.Set(field, v)
		return parser.NewBulkString([]byte(v))
	}

	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return parser.NewError("Hash value is not a float")
	}
	v = strconv.FormatFloat(inc+n, 'f', -1, 64)
	hset.Set(field, v)
	return parser.NewBulkString([]byte(v))
}

func init() {
	RegisterCmd("hset", ExecHset)
	RegisterCmd("hget", ExecHget)
	RegisterCmd("hlen", ExecHlen)
	RegisterCmd("hkeys", ExecHkeys)
	RegisterCmd("hvals", ExecHvals)
	RegisterCmd("hgetall", ExecHgetall)
	RegisterCmd("hmset", ExecHmset)
	RegisterCmd("hmget", ExecHmget)
	RegisterCmd("hexists", ExecHexists)
	RegisterCmd("hdel", ExecHdel)
	RegisterCmd("hsetnx", ExecHsetnx)
	RegisterCmd("hincrby", ExecHincrby)
	RegisterCmd("hincrbyfloat", ExecHincrbyfloat)
}
