package database

import (
	parser "simpredis/redis/resp"
	"strconv"
)

func ExecLpush(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) < 3 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	value := args[2:]

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)

	var l *List
	item, ok := engine.db.GetWithLock(key)
	if !ok {
		l = NewList()
		defer engine.db.SetWithLock(key, l)
	} else if l, ok = item.(*List); !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	for _, v := range(value) {
		l.PushHead(string(v))
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
	l, ok := item.(*List)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	node := l.PopHead()
	if node == nil {
		return parser.MakeNullBulkReply()
	}
	return parser.NewString(node.val)
}

func ExecRpush(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) < 3 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	value := args[2:]

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)

	var l *List
	item, ok := engine.db.GetWithLock(key)
	if !ok {
		l = NewList()
		defer engine.db.SetWithLock(key, l)
	} else if l, ok = item.(*List); !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	for _, v := range(value) {
		l.PushTail(string(v))
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
	l, ok := item.(*List)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	node := l.PopTail()
	if node == nil {
		return parser.MakeNullBulkReply()
	}
	return parser.NewString(node.val)
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
	l, ok := item.(*List)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	node := l.Index(index)
	if node == nil {
		return parser.MakeNullBulkReply()
	}
	return parser.NewString(node.val)
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
	l, ok := item.(*List)
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
	l, ok := item.(*List)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	nodes := l.Range(start, end)
	if nodes == nil {
		return parser.NewArray(nil)
	}
	vals := make([][]byte, len(nodes))
	for i := 0; i < len(nodes); i++ {
		vals[i] = []byte(nodes[i].val)
	}
	return parser.NewArray(vals)
}

func init() {
	RegisterCmd("lpush", ExecLpush)
	RegisterCmd("lpop", ExecLpop)
	RegisterCmd("rpush", ExecRpush)
	RegisterCmd("rpop", ExecRpop)
	RegisterCmd("lindex", ExecLindex)
	RegisterCmd("lrange", ExecLrange)
	RegisterCmd("llen", ExecLlen)
}