package database

import (
	"sync"
	"testing"
)

func TestConcurrentMap(t *testing.T) {
	conmap := NewConcurrentMap(123)
	if len(conmap.table) != 128 {
		t.Log("Capacity isn't the nearest power of 2")
		t.Fail()
	}

	var wg sync.WaitGroup
	// 测一下并发写
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(v int) {
			defer wg.Done()
			conmap.Set("123_$$", v)
		}(i)
	}
	
	wg.Wait()
	v, ok := conmap.Get("123_$$")
	if !ok {
		t.Log("Fail to set or get item")
		t.FailNow()
	}
	switch(v.(int)) {
	case 0,1,2,3,4:
		t.Logf("Set value %v", v.(int))
	default:
		t.Logf("concurrently set value failed, should be 0-4, turned to be: %v", v.(int))
		t.Fail()
	}

	conmap.SetWithLock("peter", 123)
	conmap.SetWithLock("sally", float64(456.12))
	conmap.SetWithLock("hello", "world")

	v, ok = conmap.GetWithLock("peter")
	if !ok {
		t.Logf("Fail to setWithLock or getWithLock item: %v", v)
		t.FailNow()
	}
	if v.(int) != 123 {
		t.Logf("SetWithLock or GetWithLock Fail, should get 123, turned to get: %v", v)
		t.Fail()
	} 
	v, ok = conmap.GetWithLock("sally")
	if !ok {
		t.Logf("Fail to setWithLock or getWithLock item: %v", v)
		t.FailNow()
	}
	if v.(float64) != 456.12 {
		t.Logf("SetWithLock or GetWithLock Fail, should get 456.12, turned to get: %v", v)
		t.Fail()
	} 
	v, ok = conmap.GetWithLock("hello")
	if !ok {
		t.Logf("Fail to setWithLock or getWithLock item: %v", v)
		t.FailNow()
	}
	if v.(string) != "world" {
		t.Logf("SetWithLock or GetWithLock Fail, should get \"world\", turned to get: %v", v)
		t.Fail()
	} 
}