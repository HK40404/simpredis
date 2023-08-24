package database

import (
	"bytes"
	"math"
	"strconv"
	"strings"
	"time"

	parser "github.com/HK40404/simpredis/redis/resp"
)

const (
	SETNON = iota
	SETNX
	SETXX
)

func ExecSet(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) < 3 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	value := args[2]
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
			if seconds <= 0 {
				// 时间不能为零或负数
				return parser.NewError("Invalid command format")
			}
			delayTime = time.Duration(seconds) * time.Second
		case "px":
			if i+1 >= len(args) {
				return parser.NewError("Invalid command format")
			}
			i++
			milliseconds, err := strconv.Atoi(string(args[i]))
			if err != nil {
				return parser.NewError("Invalid command format")
			}
			if milliseconds <= 0 {
				// 时间不能为零或负数
				return parser.NewError("Invalid command format")
			}
			delayTime = time.Duration(milliseconds) * time.Millisecond
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
			engine.CancelTTL(key)
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
			engine.CancelTTL(key)
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
			engine.CancelTTL(key)
		} else {
			engine.SetTTL(key, delayTime)
		}
		return parser.MakeOKReply()
	}

	return parser.NewError("Unknow error")
}

func ExecSetex(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 4 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	value := args[3]
	seconds, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return parser.NewError("Value is not an integer")
	}
	delayTime := time.Duration(seconds) * time.Second

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)
	engine.db.SetWithLock(key, value)
	engine.SetTTL(key, delayTime)
	return parser.MakeOKReply()
}

func ExecSetnx(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 3 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	value := args[2]

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)
	if _, ok := engine.db.GetWithLock(key); ok {
		return parser.NewInteger(0)
	}

	engine.db.SetWithLock(key, value)
	return parser.NewInteger(1)
}

func ExecMsetnx(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) < 3 || len(args)%2 != 1 {
		return parser.NewError("Invalid command format")
	}

	keys := make([]string, 0, len(args[1:])/2)
	vals := make([][]byte, 0, len(args[1:])/2)
	for i := 1; i < len(args); i += 2 {
		keys = append(keys, string(args[i]))
		vals = append(vals, args[i+1])
	}

	engine.lock.Locks(keys)
	defer engine.lock.UnLocks(keys)

	for _, k := range keys {
		if _, ok := engine.db.GetWithLock(k); ok {
			return parser.NewInteger(0)
		}
	}

	for i := 0; i < len(keys); i++ {
		engine.db.SetWithLock(keys[i], vals[i])
	}
	return parser.NewInteger(1)
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
	str, ok := v.([]byte)
	if !ok {
		return parser.NewError("Operation against a key holding the wrong kind of value")
	}
	return parser.NewBulkString(str)
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
		engine.db.SetWithLock(key, []byte("1"))
		return parser.NewInteger(1)
	}
	item, ok := v.([]byte)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	n, err := strconv.Atoi(string(item))
	if err != nil {
		return parser.NewError("Value is not an integer")
	}
	if n == math.MaxInt64 {
		return parser.NewError("Value is out of range")
	}
	n++
	engine.db.SetWithLock(key, []byte(strconv.Itoa(n)))
	return parser.NewInteger(int64(n))
}

func ExecIncrby(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 3 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	incr, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return parser.NewError("Value is not an integer")
	}

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)
	v, ok := engine.db.GetWithLock(key)
	if !ok {
		engine.db.SetWithLock(key, []byte(strconv.Itoa(incr)))
		return parser.NewInteger(int64(incr))
	}
	item, ok := v.([]byte)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	n, err := strconv.Atoi(string(item))
	if err != nil {
		return parser.NewError("Value is not an integer")
	}
	n += incr
	engine.db.SetWithLock(key, []byte(strconv.Itoa(n)))
	return parser.NewInteger(int64(n))
}

func ExecIncrbyfloat(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 3 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	incr, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return parser.NewError("Value is not a valid float")
	}

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)
	v, ok := engine.db.GetWithLock(key)
	if !ok {
		f := []byte(strconv.FormatFloat(incr, 'f', -1, 64))
		engine.db.SetWithLock(key, f)
		return parser.NewBulkString(f)
	}
	item, ok := v.([]byte)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	n, err := strconv.ParseFloat(string(item), 64)
	if err != nil {
		return parser.NewError("Value is not a float")
	}
	n += incr
	f := []byte(strconv.FormatFloat(n, 'f', -1, 64))
	engine.db.SetWithLock(key, f)
	return parser.NewBulkString(f)
}

func ExecDecr(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 2 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)
	v, ok := engine.db.GetWithLock(key)
	// item不存在的情况，初始化为0然后自增
	if !ok {
		engine.db.SetWithLock(key, []byte("-1"))
		return parser.NewInteger(-1)
	}
	item, ok := v.([]byte)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	n, err := strconv.Atoi(string(item))
	if err != nil {
		return parser.NewError("Value is not an integer")
	}
	if n == math.MinInt64 {
		return parser.NewError("Value is out of range")
	}
	n--
	engine.db.SetWithLock(key, []byte(strconv.Itoa(n)))
	return parser.NewInteger(int64(n))
}

func ExecDecrby(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 3 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	decr, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return parser.NewError("Value is not an integer")
	}

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)
	v, ok := engine.db.GetWithLock(key)
	if !ok {
		engine.db.SetWithLock(key, []byte(strconv.Itoa(-decr)))
		return parser.NewInteger(int64(-decr))
	}
	item, ok := v.([]byte)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	n, err := strconv.Atoi(string(item))
	if err != nil {
		return parser.NewError("Value is not an integer")
	}
	n -= decr
	engine.db.SetWithLock(key, []byte(strconv.Itoa(n)))
	return parser.NewInteger(int64(n))
}

func ExecMset(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) < 3 || len(args)%2 != 1 {
		return parser.NewError("Invalid command format")
	}
	keys := make([]string, 0, (len(args)-1)/2)
	values := make([][]byte, 0, (len(args)-1)/2)
	for i := 1; i < len(args); i += 2 {
		keys = append(keys, string(args[i]))
		values = append(values, args[i+1])
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
		v, ok := item.([]byte)
		if !ok {
			values = append(values, nil)
			continue
		}
		values = append(values, v)
	}
	return parser.NewArray(values)
}

func ExecStrlen(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 2 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	engine.lock.RLock(key)
	defer engine.lock.RUnLock(key)
	v, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.NewInteger(0)
	}
	str, ok := v.([]byte)
	if !ok {
		return parser.NewError("Operation against a key holding the wrong kind of value")
	}

	return parser.NewInteger(int64(len(str)))
}

func ExecAppend(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 3 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	value := args[2]

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)
	v, ok := engine.db.GetWithLock(key)
	if !ok {
		engine.db.SetWithLock(key, value)
		return parser.NewInteger(int64(len(value)))
	}
	s, ok := v.([]byte)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	s = append(s, value...)
	engine.db.SetWithLock(key, s)
	return parser.NewInteger(int64(len(s)))
}

func ExecGetset(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 3 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	value := args[2]

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)
	v, ok := engine.db.GetWithLock(key)
	if !ok {
		engine.db.SetWithLock(key, value)
		return parser.MakeNullBulkReply()
	}
	s, ok := v.([]byte)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	engine.db.SetWithLock(key, value)
	return parser.NewBulkString(s)
}

func ExecSetbit(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 4 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	offset, err := strconv.Atoi(string(args[2]))
	if err != nil || offset > math.MaxUint32 || offset < 0 {
		parser.NewError("Bit offset is not an integer or out of range")
	}
	var bitvalue int
	if string(args[3]) == "0" {
		bitvalue = 0
	} else if string(args[3]) == "1" {
		bitvalue = 1
	} else {
		return parser.NewError("Bit is not an integer or out of range")
	}

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)
	item, ok := engine.db.GetWithLock(key)
	if !ok {
		bmLen := offset / 8
		if offset%8 != 0 || bmLen == 0 {
			bmLen++
		}
		bm := make([]byte, bmLen)
		SetBit(&bm, offset, bitvalue)
		engine.db.SetWithLock(key, bm)
		return parser.NewInteger(0)
	}
	bm, ok := item.([]byte)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	bit := GetBit(&bm, offset)
	SetBit(&bm, offset, bitvalue)
	engine.db.SetWithLock(key, bm)
	return parser.NewInteger(int64(bit))
}

func ExecGetbit(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 3 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	offset, err := strconv.Atoi(string(args[2]))
	if err != nil || offset > math.MaxUint32 || offset < 0 {
		parser.NewError("Bit offset is not an integer or out of range")
	}

	engine.lock.RLock(key)
	defer engine.lock.RUnLock(key)
	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.NewInteger(0)
	}
	bm, ok := item.([]byte)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	bit := GetBit(&bm, offset)
	return parser.NewInteger(int64(bit))
}

func ExecBitcount(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 2 && len(args) != 4 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	var start, end int
	if len(args) == 4 {
		var err error
		start, err = strconv.Atoi(string(args[2]))
		if err != nil {
			parser.NewError("Start is not an integer or out of range")
		}
		end, err = strconv.Atoi(string(args[3]))
		if err != nil {
			parser.NewError("End is not an integer or out of range")
		}
	}

	engine.lock.RLock(key)
	defer engine.lock.RUnLock(key)
	item, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.NewInteger(0)
	}
	bm, ok := item.([]byte)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if len(args) == 2 {
		start = 0
		end = len(bm) - 1
	} else {
		if start < 0 {
			start += len(bm)
			if start < 0 {
				start = 0
			}
		} else if start >= len(bm) {
			return parser.NewInteger(0)
		}
		if end < 0 {
			end += len(bm)
			if end < 0 {
				return parser.NewInteger(0)
			}
		} else if end >= len(bm) {
			end = len(bm) - 1
		}
		if start > end {
			return parser.NewInteger(0)
		}
	}

	startOffset := start * 8
	endOffset := end*8 + 7
	count := BitCount(&bm, startOffset, endOffset)
	return parser.NewInteger(int64(count))
}

func ExecBitop(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) < 4 {
		return parser.NewError("Invalid command format")
	}

	op := strings.ToLower(string(args[1]))
	switch op {
	case "and", "or", "xor":
	case "not":
		if len(args) != 4 {
			return parser.NewError("Invalid command format")
		}
	default:
		return parser.NewError("Invalid command format")
	}
	dstkey := string(args[2])
	keys := make([]string, 0, len(args[3:]))
	for i := 3; i < len(args); i++ {
		keys = append(keys, string(args[i]))
	}

	engine.lock.RWLocks(keys, []string{dstkey})
	defer engine.lock.RWUnLocks(keys, []string{dstkey})

	vals := make([][]byte, 0, len(keys))
	for _, k := range keys {
		item, ok := engine.db.GetWithLock(k)
		if !ok {
			vals = append(vals, nil)
			continue
		}
		bm, ok := item.([]byte)
		if !ok {
			return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		vals = append(vals, bm)
	}
	res := BitOp(op, vals)
	engine.db.SetWithLock(dstkey, res)
	engine.CancelTTL(dstkey)
	return parser.NewInteger(int64(len(res)))
}

func ExecSetrange(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 4 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	offset, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return parser.NewError("Value is not an integer or out of range")
	}
	value := args[3]

	engine.lock.Lock(key)
	defer engine.lock.UnLock(key)

	v, ok := engine.db.GetWithLock(key)
	if !ok {
		res := make([]byte, offset)
		res = append(res, value...)
		engine.db.SetWithLock(key, res)
		return parser.NewInteger(int64(len(res)))
	}
	s, ok := v.([]byte)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	buf := bytes.Buffer{}
	buf.Write(s[:offset])
	buf.Write(value)
	buf.Write(s[offset+len(value):])

	engine.db.SetWithLock(key, buf.Bytes())
	return parser.NewInteger(int64(buf.Len()))
}

func ExecGetrange(engine *DBEngine, args [][]byte) parser.RespData {
	if len(args) != 4 {
		return parser.NewError("Invalid command format")
	}

	key := string(args[1])
	start, err := strconv.Atoi(string(args[2]))
	if err != nil {
		return parser.NewError("Value is not an integer or out of range")
	}
	end, err := strconv.Atoi(string(args[3]))
	if err != nil {
		return parser.NewError("Value is not an integer or out of range")
	}

	engine.lock.RLock(key)
	defer engine.lock.RUnLock(key)

	v, ok := engine.db.GetWithLock(key)
	if !ok {
		return parser.NewBulkString(make([]byte, 0))
	}
	bm, ok := v.([]byte)
	if !ok {
		return parser.NewError("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if start < 0 {
		start += len(bm)
		if start < 0 {
			start = 0
		}
	} else if start >= len(bm) {
		return parser.NewBulkString(make([]byte, 0))
	}
	if end < 0 {
		end += len(bm)
		if end < 0 {
			return parser.NewBulkString(make([]byte, 0))
		}
	} else if end >= len(bm) {
		end = len(bm) - 1
	}
	if start > end {
		return parser.NewBulkString(make([]byte, 0))
	}

	return parser.NewBulkString(bm[start : end+1])
}

func init() {
	RegisterCmd("set", ExecSet)
	RegisterCmd("setex", ExecSetex)
	RegisterCmd("setnx", ExecSetnx)
	RegisterCmd("getset", ExecGetset)
	RegisterCmd("get", ExecGet)
	RegisterCmd("mset", ExecMset)
	RegisterCmd("mget", ExecMget)
	RegisterCmd("msetnx", ExecMsetnx)
	RegisterCmd("incr", ExecIncr)
	RegisterCmd("incrby", ExecIncrby)
	RegisterCmd("incrbyfloat", ExecIncrbyfloat)
	RegisterCmd("decr", ExecDecr)
	RegisterCmd("decrby", ExecDecrby)
	RegisterCmd("strlen", ExecStrlen)
	RegisterCmd("append", ExecAppend)
	RegisterCmd("setbit", ExecSetbit)
	RegisterCmd("getbit", ExecGetbit)
	RegisterCmd("bitcount", ExecBitcount)
	RegisterCmd("bitop", ExecBitop)
	RegisterCmd("setrange", ExecSetrange)
	RegisterCmd("getrange", ExecGetrange)
}
