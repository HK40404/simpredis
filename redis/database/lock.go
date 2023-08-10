package database

import (
	hash "simpredis/utils"
	"sort"
	"sync"
)

type ItemsLock struct {
	l []sync.RWMutex
}

func NewItemsLock(lockCount int) *ItemsLock {
	lockCount = GetCompacity(lockCount)
	itemlock := &ItemsLock{ l: make([]sync.RWMutex, lockCount) }
	return itemlock
}

func (lock *ItemsLock) spread(key string) int {
	hash := hash.Fnv(key)
	hash = uint32(len(lock.l)-1) & hash
	return int(hash)
}

func (lock *ItemsLock) Lock(key string) {
	index := lock.spread(key)
	lock.l[index].Lock()
}

func (lock *ItemsLock) UnLock(key string) {
	index := lock.spread(key)
	lock.l[index].Unlock()
}

func (lock *ItemsLock) RLock(key string) {
	index := lock.spread(key)
	lock.l[index].RLock()
}

func (lock *ItemsLock) RUnLock(key string) {
	index := lock.spread(key)
	lock.l[index].RUnlock()
}

// 返回排好序且唯一的索引列表
func (lock *ItemsLock) indicesFromKeys(keys []string) []int {
	m := make(map[int]struct{})
	// 防止一个锁锁两次，造成死循环
	for _, v := range keys {
		index := lock.spread(v)
		m[index] = struct{}{}
	}
	indices := make([]int, 0, len(m))
	for index := range m {
		indices = append(indices, index)
	}
	sort.Ints(indices)
	return indices
}

func (lock *ItemsLock) Locks(keys []string) {
	indices := lock.indicesFromKeys(keys)
	for _, index := range indices {
		lock.l[index].Lock()
	}
}

func (lock *ItemsLock) UnLocks(keys []string) {
	indices := lock.indicesFromKeys(keys)
	for _, index := range indices {
		lock.l[index].Unlock()
	}
}

func (lock *ItemsLock) RLocks(keys []string) {
	indices := lock.indicesFromKeys(keys)
	for _, index := range indices {
		lock.l[index].RLock()
	}
}

func (lock *ItemsLock) RUnLocks(keys []string) {
	indices := lock.indicesFromKeys(keys)
	for _, index := range indices {
		lock.l[index].RUnlock()
	}
}