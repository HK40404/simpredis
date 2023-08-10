package database

import (
	parser "simpredis/redis/resp"
	"simpredis/utils/config"
	"simpredis/utils/logger"
	"strconv"
	"time"
)

const (
	NOEXPIRED = iota
	EXPIRED
	NOEXIST
)

type DBEngine struct {
	db *ConcurrentMap		// 实际存储数据的db
	ttldb *ConcurrentMap	// 保存item过期时间的db
	lock *ItemsLock			// 可以锁多个item的锁，用于原子性修改多个值
}

func NewDBEngine() *DBEngine {
	shardCount, err := strconv.Atoi(config.Cfg.ShardCount)
	if err != nil {
		logger.Warn("Invalid shardcount from config, set shardcount = 16")
		shardCount = 16
	}
	return &DBEngine{
		db: NewConcurrentMap(shardCount),
		ttldb: NewConcurrentMap(shardCount),
		lock: NewItemsLock(shardCount*4),
	}
}

func (engine *DBEngine) ExecCmd(array [][]byte) parser.RespData {
	execFunc, ok := CmdTable[string(array[0])]
	if !ok {
		return parser.NewError("Unsupported command")
	}
	return execFunc(engine, array)
}

// 要存在item，才能设置它的过期时间
func (engine *DBEngine) SetTTL(key string, expireTime int64) bool {
	if _, ok := engine.db.Get(key); ok {
		_, exist := engine.ttldb.Get(key)
		if !exist {
			engine.ttldb.count++
		}
		engine.ttldb.Set(key, expireTime)
		return true
	}
	return false
}

// 查询item时才调用checkTTL，删除相应item
func (engine *DBEngine) CheckTTL(key string) int {
	expireTime, ok := engine.ttldb.Get(key)
	if !ok {
		// key不存在
		return NOEXIST
	}
	// 还没过期
	if expireTime.(int64) > time.Now().Unix() {
		return NOEXPIRED
	} else {
		// 过期了
		engine.lock.Lock(key)
		defer engine.lock.UnLock(key)
		engine.db.DelWithLock(key)
		engine.ttldb.Del(key)
		return EXPIRED
	}
}

func (engine *DBEngine) DelTTL(key string) bool {
	if _, ok := engine.ttldb.Get(key); ok {
		engine.ttldb.count--
		engine.ttldb.Del(key)
		return true
	}
	return false
}