package database

import (
	"strconv"

	parser "github.com/HK40404/simpredis/redis/resp"
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

	count := 0
	for _, m := range members {
		if set.IsMember(string(m)) {
			continue
		}
		count++
		set.Add(string(m))
	}
	return parser.NewInteger(int64(count))
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
	if set.Len() == 0 {
		engine.db.DelWithLock(key)
		engine.CancelTTL(key)
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
			return parser.NewArray(nil)
		}
		set, ok := item.(*Set)
		if !ok {
			return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		sets = append(sets, set)
	}

	members := make([][]byte, 0, len(sets[0].s))
	for _, m := range Inter(sets) {
		members = append(members, []byte(m))
	}
	return parser.NewArray(members)
}

func ExecSinterstore(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) < 3 {
		return parser.NewError("Invalid command format")
	}

	storekey := string(args[1])
	keys := make([]string, 0, len(args[2:]))
	for _, k := range args[2:] {
		keys = append(keys, string(k))
	}

	engine.lock.RWLocks(keys, []string{storekey})
	defer engine.lock.RWUnLocks(keys, []string{storekey})

	sets := make([]*Set, 0, len(keys))
	for _, k := range keys {
		item, ok := engine.db.GetWithLock(k)
		if !ok {
			return parser.NewArray(nil)
		}
		set, ok := item.(*Set)
		if !ok {
			return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		sets = append(sets, set)
	}

	storeset := NewSet()
	for _, m := range Inter(sets) {
		storeset.Add(m)
	}
	engine.CancelTTL(storekey)
	engine.db.SetWithLock(storekey, storeset)

	return parser.NewInteger(int64(storeset.Len()))
}

func ExecSpop(engine *DBEngine, args [][]byte) parser.RespData {
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
	if set.Len() == 0 {
		engine.db.DelWithLock(key)
		engine.CancelTTL(key)
	}

	return parser.NewBulkString([]byte(str))
}

func ExecSrandmember(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 2 && len(args) != 3 {
		return parser.NewError("Invalid command format")
	}
	key := string(args[1])
	count := 1
	if len(args) == 3 {
		var err error
		count, err = strconv.Atoi(string(args[2]))
		if err != nil {
			return parser.NewError("Count is not an integer")
		}
	}

	engine.lock.RLock(key)
	defer engine.lock.RUnLock(key)
	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.MakeNullBulkReply()
	}
	set, ok := item.(*Set)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	memstrs := set.RandMem(count)
	if memstrs == nil {
		return parser.NewArray(nil)
	}
	members := make([][]byte, 0, len(memstrs))
	for _, s := range memstrs {
		members = append(members, []byte(s))
	}
	return parser.NewArray(members)
}

func ExecSdiff(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) < 2 {
		return parser.NewError("Invalid command format")
	}
	keys := make([]string, 0, len(args[1:]))
	for _, k := range args[1:] {
		keys = append(keys, string(k))
	}

	engine.lock.RLocks(keys)
	defer engine.lock.RUnLocks(keys)

	sets := make([]*Set, 0, len(keys))

	for _, k := range keys {
		item, ok := engine.db.GetWithLock(k)
		if !ok {
			sets = append(sets, nil)
			continue
		}
		set, ok := item.(*Set)
		if !ok {
			return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		sets = append(sets, set)
	}

	if sets[0] == nil {
		return parser.NewArray(nil)
	}
	diff := make([][]byte, 0, sets[0].Len()/2)
	sets[0].ForEach(func(s string) bool {
		isInter := false
		for i := 1; i < len(sets); i++ {
			if sets[i] != nil && sets[i].IsMember(s) {
				isInter = true
				break
			}
		}
		if !isInter {
			diff = append(diff, []byte(s))
		}
		return true
	})
	return parser.NewArray(diff)
}

func ExecSdiffstore(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) < 3 {
		return parser.NewError("Invalid command format")
	}
	storekey := string(args[1])
	keys := make([]string, 0, len(args[2:]))
	for _, v := range args[2:] {
		keys = append(keys, string(v))
	}

	engine.lock.RWLocks(keys, []string{storekey})
	defer engine.lock.RWUnLocks(keys, []string{storekey})

	sets := make([]*Set, 0, len(keys))
	for _, k := range keys {
		item, ok := engine.db.GetWithLock(k)
		if !ok {
			sets = append(sets, nil)
			continue
		}
		set, ok := item.(*Set)
		if !ok {
			return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		sets = append(sets, set)
	}

	if sets[0] == nil {
		// empty set
		engine.CancelTTL(storekey)
		engine.db.DelWithLock(storekey)
		return parser.NewInteger(0)
	}
	diff := make([]string, 0, sets[0].Len()/2)
	sets[0].ForEach(func(s string) bool {
		isInter := false
		for i := 1; i < len(sets); i++ {
			if sets[i] != nil && sets[i].IsMember(s) {
				isInter = true
				break
			}
		}
		if !isInter {
			diff = append(diff, s)
		}
		return true
	})

	storeset := NewSet()
	for _, m := range diff {
		storeset.Add(m)
	}
	engine.CancelTTL(storekey)
	engine.db.SetWithLock(storekey, storeset)

	return parser.NewInteger(int64(storeset.Len()))
}

func ExecSmove(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 4 {
		return parser.NewError("Invalid command format")
	}

	srckey := string(args[1])
	dstkey := string(args[2])
	member := string(args[3])

	engine.lock.Locks([]string{srckey, dstkey})
	defer engine.lock.UnLocks([]string{srckey, dstkey})

	srcitem, ok := engine.db.GetWithLock(srckey)
	if !ok {
		return parser.NewInteger(0)
	}
	srcset, ok := srcitem.(*Set)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if !srcset.Remove(member) {
		return parser.NewInteger(0)
	}
	if srcset.Len() == 0 {
		engine.db.DelWithLock(srckey)
		engine.CancelTTL(srckey)
	}

	var dstset *Set
	item, ok := engine.db.GetWithLock(dstkey)
	if !ok {
		dstset = NewSet()
		defer engine.db.SetWithLock(dstkey, dstset)
	} else {
		dstset, ok = item.(*Set)
		if !ok {
			return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}

	if dstset.IsMember(member) {
		return parser.NewInteger(1)
	}
	dstset.Add(member)
	return parser.NewInteger(1)
}

func ExecSunion(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) < 2 {
		return parser.NewError("Invalid command format")
	}

	keys := make([]string, 0, len(args[1:]))
	for _, k := range args[1:] {
		keys = append(keys, string(k))
	}

	engine.lock.RLocks(keys)
	defer engine.lock.RUnLocks(keys)

	var sets []*Set
	for _, k := range keys {
		item, ok := engine.db.GetWithLock(k)
		if !ok {
			sets = append(sets, nil)
			continue
		}
		set, ok := item.(*Set)
		if !ok {
			return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		sets = append(sets, set)
	}

	union := Union(sets)
	members := make([][]byte, 0, len(union))
	for _, m := range union {
		members = append(members, []byte(m))
	}
	return parser.NewArray(members)
}

func ExecSunionStore(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) < 3 {
		return parser.NewError("Invalid command format")
	}

	dstkey := string(args[1])
	keys := make([]string, 0, len(args[2:]))
	for _, k := range args[1:] {
		keys = append(keys, string(k))
	}

	engine.lock.RWLocks(keys, []string{dstkey})
	defer engine.lock.RWUnLocks(keys, []string{dstkey})

	var sets []*Set
	for _, k := range keys {
		item, ok := engine.db.GetWithLock(k)
		if !ok {
			sets = append(sets, nil)
			continue
		}
		set, ok := item.(*Set)
		if !ok {
			return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		sets = append(sets, set)
	}

	dstset := NewSet()
	union := Union(sets)
	for _, m := range union {
		dstset.Add(m)
	}
	engine.CancelTTL(dstkey)
	engine.db.SetWithLock(dstkey, dstset)
	return parser.NewInteger(int64(dstset.Len()))
}

func init() {
	RegisterCmd("sadd", ExecSadd)
	RegisterCmd("scard", ExecScard)
	RegisterCmd("smembers", ExecSmembers)
	RegisterCmd("srem", ExecSrem)
	RegisterCmd("sismember", ExecSismember)
	RegisterCmd("sinter", ExecSinter)
	RegisterCmd("sinterstore", ExecSinterstore)
	RegisterCmd("spop", ExecSpop)
	RegisterCmd("srandmember", ExecSrandmember)
	RegisterCmd("sdiff", ExecSdiff)
	RegisterCmd("sdiffstore", ExecSdiffstore)
	RegisterCmd("smove", ExecSmove)
	RegisterCmd("sunion", ExecSunion)
	RegisterCmd("sunionstore", ExecSunionStore)
}
