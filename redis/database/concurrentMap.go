package database

import (
	"math"
	hash "simpredis/utils"
	"sync"
)

type ConcurrentMap struct {
	table []*Shard
	count int
}

type Shard struct {
	m     map[string]any
	mutex sync.RWMutex
}

// 将参数变为二的幂，方便哈希后取模
func GetCompacity(num int) int {
	if num < 16 {
		return 16
	}
	n := num - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n >= math.MaxUint32 {
		return math.MaxUint32
	}
	return int(n + 1)
}

func NewConcurrentMap(shardCount int) *ConcurrentMap {
	shardCount = GetCompacity(shardCount)
	conmap := &ConcurrentMap{
		table: make([]*Shard, shardCount),
		count: 0,
	}
	for i := range conmap.table {
		conmap.table[i] = &Shard{ m: make(map[string]any)}
	}
	return conmap
}

// 将key hash成map索引
func (conmap *ConcurrentMap) spread(key string) int {
	hash := hash.Fnv(key)
	hash = uint32(len(conmap.table)-1) & hash
	return int(hash)
}

func (conmap *ConcurrentMap) Get(key string) (any, bool) {
	idx := conmap.spread(key)
	table := conmap.table[idx]
	table.mutex.RLock()
	defer table.mutex.RUnlock()
	value, ok := table.m[key]
	if !ok {
		return nil, false
	}
	return value, true
}

func (conmap *ConcurrentMap) Set(key string, value any) {
	idx := conmap.spread(key)
	table := conmap.table[idx]
	table.mutex.Lock()
	defer table.mutex.Unlock()
	if _, ok := table.m[key]; ok {
		table.m[key] = value
	} else {
		table.m[key] = value
		conmap.count++
	}
}

func (conmap *ConcurrentMap) Del(key string) {
	idx := conmap.spread(key)
	table := conmap.table[idx]
	table.mutex.Lock()
	defer table.mutex.Unlock()
	if _, ok := table.m[key]; ok {
		conmap.count--
	}
	delete(table.m, key)
}

func (conmap *ConcurrentMap) GetWithLock(key string) (any, bool) {
	idx := conmap.spread(key)
	table := conmap.table[idx]
	value, ok := table.m[key]
	if !ok {
		return nil, false
	}
	return value, true
}

func (conmap *ConcurrentMap) SetWithLock(key string, value any) {
	idx := conmap.spread(key)
	table := conmap.table[idx]
	if _, ok := table.m[key]; ok {
		table.m[key] = value
	} else {
		table.m[key] = value
		conmap.count++
	}
}

func (conmap *ConcurrentMap) DelWithLock(key string) {
	idx := conmap.spread(key)
	table := conmap.table[idx]
	if _, ok := table.m[key]; ok {
		conmap.count--
	}
	delete(table.m, key)
}