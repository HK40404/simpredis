package database

import (
	parser "simpredis/redis/resp"
	"simpredis/utils/config"
	"simpredis/utils/logger"
	"simpredis/utils/timewheel"
	"strconv"
	"strings"
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
	cmd := strings.ToLower(string(array[0]))
	execFunc, ok := CmdTable[cmd]
	if !ok {
		return parser.NewError("Unsupported command")
	}
	return execFunc(engine, array)
}

func (engine *DBEngine) SetTTL(key string, delayTime time.Duration) bool {
	if delayTime < time.Duration(0) {
		return false
	}

	_, exist := engine.ttldb.Get(key)
	if !exist {
		engine.ttldb.count++
	}
	engine.ttldb.Set(key, time.Now().Add(delayTime).Unix())
	// 要先把之前的定时任务删除
	timewheel.Tw.RemoveTask(key)
	job := func () {
		engine.lock.Lock(key)
		defer engine.lock.UnLock(key)
		engine.db.DelWithLock(key)
		engine.DelTTL(key)
	}
	timewheel.Tw.AddTask(key, delayTime, job)
	return true
}

func(engine *DBEngine) CancelTTL(key string) {
	timewheel.Tw.RemoveTask(key)
	engine.DelTTL(key)
}

func (engine *DBEngine) DelTTL(key string) bool {
	if _, ok := engine.ttldb.Get(key); ok {
		engine.ttldb.count--
		engine.ttldb.Del(key)
		return true
	}
	return false
}