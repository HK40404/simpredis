package database

import (
	parser "simpredis/redis/resp"
)

func ExecSadd(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) < 3 {
		return parser.NewError("Invalid command format")
	}
	key := string(args[1])
	members := args[2:]

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)

	var set *Set
	item, ok := engine.db.GetWithLock(key)
	if !ok {
		set = NewSet()
		defer engine.db.SetWithLock(key, set)
	} else {
		set, ok = item.(*Set)
		if !ok {
			return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}

	for _, m := range members {
		set.Add(string(m))
	}
	return parser.NewInteger(int64(len(members)))
}

func ExecScard(engine *DBEngine, args [][]byte) parser.RespData {
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
	set, ok := item.(*Set)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return parser.NewInteger(int64(len(set.s)))
}

func ExecSmembers(engine *DBEngine, args [][]byte) parser.RespData {
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
	set, ok := item.(*Set)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	members := make([][]byte, 0, set.Len())
	for _, m := range set.Members() {
		members = append(members, []byte(m))
	}

	return parser.NewArray(members)
}

func ExecSrem(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) < 3 {
		return parser.NewError("Invalid command format")
	}
	key := string(args[1])
	members := args[2:]

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)
	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.NewInteger(0)
	}
	set, ok := item.(*Set)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	delCount := 0
	for _, m := range members {
		if set.Remove(string(m)) {
			delCount++
		}
	}
	return parser.NewInteger(int64(delCount))
}

func ExecSismember(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 3 {
		return parser.NewError("Invalid command format")
	}
	key := string(args[1])
	member := string(args[2])

	engine.lock.RLock(key)
	defer engine.lock.RUnLock(key)
	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.NewInteger(0)
	}
	set, ok := item.(*Set)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if set.IsMember(member) {
		return parser.NewInteger(1)
	}

	return parser.NewInteger(0)
}

func ExecSinter(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) < 2 {
		return parser.NewError("Invalid command format")
	}
	keys := make([]string, 0, len(args[1:]))
	for _, k := range args[1:] {
		keys = append(keys, string(k))
	}
	sets := make([]*Set, 0, len(keys))
	engine.lock.RLocks(keys)
	defer engine.lock.RUnLocks(keys)
	for _, k := range keys {
		item, ok := engine.db.GetWithLock(k)
		if !ok {
			return parser.NewString("(empty set)")
		}
		set, ok := item.(*Set)
		if !ok {
			return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		sets = append(sets, set)
	}

	members := make([][]byte, 0, len(sets[0].s))
	for _, m := range InterSets(sets) {
		members = append(members, []byte(m))
	}
	return parser.NewArray(members)
}

func ExecSPop(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 2 {
		return parser.NewError("Invalid command format")
	}
	key := string(args[1])

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)
	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.MakeNullBulkReply()
	}
	set, ok := item.(*Set)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	str := set.Pop()
	if str == "" {
		return parser.MakeNullBulkReply()
	}
	return parser.NewBulkString([]byte(str))
}

func init() {
	RegisterCmd("sadd", ExecSadd)
	RegisterCmd("scard", ExecScard)
	RegisterCmd("smembers", ExecSmembers)
	RegisterCmd("srem", ExecSrem)
	RegisterCmd("sismember", ExecSismember)
	RegisterCmd("sinter", ExecSinter)
	RegisterCmd("spop", ExecSPop)
}
