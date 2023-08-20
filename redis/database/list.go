package database

import (
	"bytes"
	parser "simpredis/redis/resp"
	"strconv"
	"strings"
)

func ExecLpush(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) < 3 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	values := args[2:]

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)

	var l *QuickList
	item, ok := engine.db.GetWithLock(key)
	if !ok {
		l = NewQuickList()
		defer engine.db.SetWithLock(key, l)
	} else if l, ok = item.(*QuickList); !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	for _, v := range values {
		l.Insert(0, v)
	}
	length := l.Len()
	return parser.NewInteger(int64(length))
}

func ExecLpop(engine *DBEngine, args [][]byte) parser.RespData {
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
	l, ok := item.(*QuickList)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	val := l.RemoveByIndex(0)
	if l.Len() == 0 {
		engine.db.DelWithLock(key)
		engine.CancelTTL(key)
	}
	return parser.NewBulkString(val)
}

func ExecRpush(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) < 3 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	value := args[2:]

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)

	var l *QuickList
	item, ok := engine.db.GetWithLock(key)
	if !ok {
		l = NewQuickList()
		defer engine.db.SetWithLock(key, l)
	} else if l, ok = item.(*QuickList); !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	for _, v := range value {
		l.PushBack(v)
	}
	length := l.Len()
	return parser.NewInteger(int64(length))
}

func ExecRpop(engine *DBEngine, args [][]byte) parser.RespData {
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
	l, ok := item.(*QuickList)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	val := l.RemoveByIndex(-1)
	if l.Len() == 0 {
		engine.db.DelWithLock(key)
		engine.CancelTTL(key)
	}
	return parser.NewBulkString(val)
}

func ExecLindex(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 3 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])

	index, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return parser.NewError("Value is not an integer")
	}
	engine.lock.RLock(key)
	defer engine.lock.RUnLock(key)
	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.MakeNullBulkReply()
	}
	l, ok := item.(*QuickList)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	val := l.GetByIndex(index)
	if val == nil {
		return parser.MakeNullBulkReply()
	}
	return parser.NewBulkString(val)
}

func ExecLlen(engine *DBEngine, args [][]byte) parser.RespData {
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
	l, ok := item.(*QuickList)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return parser.NewInteger(int64(l.Len()))
}

func ExecLrange(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 4 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])

	start, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return parser.NewError("Start index is not an integer")
	}
	end, err := strconv.Atoi(string(args[3]))
	if err != nil {
		return parser.NewError("End index is not an integer")
	}

	engine.lock.RLock(key)
	defer engine.lock.RUnLock(key)
	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.NewArray(nil)
	}
	l, ok := item.(*QuickList)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	vals := l.Range(start, end)
	if vals == nil {
		return parser.NewArray(nil)
	}
	return parser.NewArray(vals)
}

func ExecLset(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 4 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	index, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return parser.NewError("Index value is not an integer")
	}

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)
	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.NewError("No such key")
	}
	l, ok := item.(*QuickList)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if !l.Set(index, args[3]) {
		return parser.NewError("Index out of range")
	}
	return parser.MakeOKReply()
}

func ExecLpushX(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 3 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)

	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.NewInteger(0)
	} 
	l, ok := item.(*QuickList)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	l.Insert(0, args[2])
	length := l.Len()
	return parser.NewInteger(int64(length))
}

func ExecRpushX(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 3 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)

	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.NewInteger(0)
	} 
	l, ok := item.(*QuickList)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	l.PushBack(args[2])
	length := l.Len()
	return parser.NewInteger(int64(length))
}

func ExecRpopLpush(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 3 {
		return parser.NewError("Invalid command format")
	}

	keys := make([]string, 2)
	keys[0] = string(args[1])
	keys[1] = string(args[2])

	engine.lock.Locks(keys)
	defer engine.lock.UnLocks(keys)

	srcitem, ok := engine.db.GetWithLock(keys[0])
	if !ok {
		return parser.MakeNullBulkReply()
	} 
	srcl, ok := srcitem.(*QuickList)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	
	dstitem, ok := engine.db.GetWithLock(keys[1])
	var dstl *QuickList
	if !ok {
		dstl = NewQuickList()
		defer engine.db.SetWithLock(keys[1], dstl)
	} else if dstl, ok = dstitem.(*QuickList); !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	val := srcl.RemoveByIndex(-1)
	if srcl.Len() == 0 {
		engine.db.DelWithLock(keys[0])
		engine.CancelTTL(keys[0])
	}

	dstl.Insert(0, val)
	return parser.NewBulkString(val)
}

func ExecLinsert(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 5  {
		return parser.NewError("Invalid command format")
	}

	op := strings.ToLower(string(args[2]))
	if op != "before" && op != "after" {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	pivot := args[3]
	value := args[4]

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)

	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.NewInteger(0)
	} 
	l, ok := item.(*QuickList)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	pivotIndex := -1
	l.ForEach(func(i int, a any) bool {
		if bytes.Equal(a.([]byte), pivot) {
			pivotIndex = i
			return false
		}
		return true
	})
	// 没有找到pivot
	if pivotIndex == -1 {
		return parser.NewInteger(-1)
	}

	switch op {
	case "before":
		l.Insert(pivotIndex, value)
	case "after":
		l.Insert(pivotIndex+1, value)
	}

	return parser.NewInteger(int64(l.Len()))
}

func ExecLrem(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 4  {
		return parser.NewError("Invalid command format")
	}

	count, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return parser.NewError("Value is not an integer")
	}

	key := string(args[1])

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)

	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.NewInteger(0)
	} 
	l, ok := item.(*QuickList)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	n := l.RemoveByCount(args[3], count)
	if l.Len() == 0 {
		engine.db.DelWithLock(key)
		engine.CancelTTL(key)
	}
	return parser.NewInteger(int64(n))
}

func ExecLtrim(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 4  {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	start, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return parser.NewError("Start index is not an integer")
	}
	stop, err := strconv.Atoi(string(args[3]))
	if err != nil {
		return parser.NewError("Stop index is not an integer")
	}

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)

	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.MakeOKReply()
	} 
	l, ok := item.(*QuickList)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	// 如果start>stop或者start、stop超出了范围：
	// 删除所有元素
	removeAll := false
	if start < 0 {
		start += l.len
		if start < 0 {
			start = 0
		}
	} else if start >= l.len {
		removeAll =true
	}
	if stop < 0 {
		stop += l.len
		if stop < 0 {
			removeAll = true
		}
	} else if stop >= l.len {
		stop = l.len - 1
	}

	if start > stop {
		removeAll = true
	}

	if removeAll {
		iter := l.Find(0)
		for !iter.atEnd() {
			iter.remove()
		}
		return parser.MakeOKReply()
	}

	iter := l.Find(0)
	i := 0
	for !iter.atEnd() {
		if i >= start && i <= stop {
			iter.next()
		} else {
			iter.remove()
		}
		i++
	}

	if l.Len() == 0 {
		engine.db.DelWithLock(key)
		engine.CancelTTL(key)
	}
	return parser.MakeOKReply()
}

func init() {
	RegisterCmd("lpush", ExecLpush)
	RegisterCmd("lpop", ExecLpop)
	RegisterCmd("rpush", ExecRpush)
	RegisterCmd("rpop", ExecRpop)
	RegisterCmd("lindex", ExecLindex)
	RegisterCmd("lrange", ExecLrange)
	RegisterCmd("llen", ExecLlen)
	RegisterCmd("lset", ExecLset)
	RegisterCmd("lpushx", ExecLpushX)
	RegisterCmd("rpushx", ExecRpushX)
	RegisterCmd("rpoplpush", ExecRpopLpush)
	RegisterCmd("linsert", ExecLinsert)
	RegisterCmd("lrem", ExecLrem)
	RegisterCmd("ltrim", ExecLtrim)
}