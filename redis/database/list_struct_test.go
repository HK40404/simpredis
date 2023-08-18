package database

import (
	"bytes"
	"fmt"
	"testing"
)


func PrintQLByPage(ql *QuickList) {
	for e := ql.l.Front(); ; e = e.Next() {
		page := e.Value.([]any)
		for _, n := range page {
			fmt.Printf("%d ", n.(int))
		}
		fmt.Printf("\tlen: %d cap: %d\n", len(page), cap(page))
		if e == ql.l.Back() {
			break
		}
	}
}

func TestList(t *testing.T) {
	PAGESIZE = 4
	ql := NewQuickList()
	
	// l: [0 1 2 3 4 5 6 7 8 9]
	// test pushback
	for i := 0; i < 10; i++ {
		ql.PushBack(i)
	}

	// test find
	for i := 0; i < 10; i++ {
		n := ql.Find(i).get().(int)
		if n != i {
			t.Logf("Find %d ele wrong", i)
			t.Fail()
		}
	}

	// test insert
	ql.Insert(4, 33)
	ql.Insert(10, 88)
	ql.Insert(0, -1)
	ql.Insert(ql.Len(), 10)
	ql.Insert(9, 555)
	// l: [-1 0 1 2 3 33 4 5 6 555 7 8 88 9 10]
	PrintQLByPage(ql)
	
	if ql.Find(0).get().(int) != -1 {
		t.Log("Find first ele wrong")
		t.Fail()
	}
	if ql.Find(5).get().(int) != 33 {
		t.Log("Find 5th ele wrong")
		t.Fail()
	}
	if ql.Find(12).get().(int) != 88 {
		t.Log("Find 11th ele wrong")
		t.Fail()
	}
	if ql.Find(ql.Len()-1).get().(int) != 10 {
		t.Log("Find last ele wrong")
		t.Fail()
	}
	if ql.Find(9).get().(int) != 555 {
		t.Log("Find last ele wrong")
		t.Fail()
	}

	ql = NewQuickList()
	for i := 0; i < PAGESIZE; i++ {
		ql.PushBack([]byte("123"))
	}
	ql.PushBack([]byte("222"))
	ql.PushBack([]byte("223"))

	// ql: [ 123 123 123 123 222 223 ]
	ql.removeAll([]byte("2"))
	ql.removeAll([]byte("222"))
	if !bytes.Equal(ql.Find(ql.Len()-1).get().([]byte), []byte("223")) {
		t.Fail()
	}

	// ql: [ 123 123 123 123 223 ]
	ql.removeCount([]byte("123"), PAGESIZE/2)
	if !bytes.Equal(ql.Find(0).get().([]byte), []byte("123")) {
		t.Fail()
	}
	// ql: [ 123 123 223 ]
	ql.removeCount([]byte("123"), PAGESIZE/2)
	if !bytes.Equal(ql.Find(0).get().([]byte), []byte("223")) {
		t.Fail()
	}

	ql.PushBack([]byte("999"))
	ql.PushBack([]byte("888"))
	ql.PushBack([]byte("999"))
	// ql: [ 223 999 888 999 ]
	ql.removeCountReverse([]byte("123"), PAGESIZE/2)
	ql.removeCountReverse([]byte("999"), 1)
	// ql: [ 223 999 888 ]
	if !bytes.Equal(ql.Find(1).get().([]byte), []byte("999")) {
		t.Fail()
	}

	vals := ql.Range(1,23)
	if !bytes.Equal(vals[0], []byte("999")) {
		t.Fail()
	}
	if !bytes.Equal(vals[1], []byte("888")) {
		t.Fail()
	}
}