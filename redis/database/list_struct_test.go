package database

import (
	"strconv"
	"testing"
)

func TestList(t *testing.T) {
	l := NewList()
	
	// l: [0 1 2 3 4 5 6 7 8 9]
	// test push tail
	for i := 5; i < 10; i++ {
		l.PushTail(strconv.Itoa(i))
	}
	// test push head
	for i := 4; i >= 0; i-- {
		l.PushHead(strconv.Itoa(i))
	}
	if l.Len() != 10 {
		t.Fail()
	}

	// test index
	if l.Index(-11) != nil {
		t.Log("wrong result of List.Index()")
		t.Fail()
	}
	if l.Index(10) != nil {
		t.Log("wrong result of List.Index()")
		t.Fail()
	}
	if l.Index(-10) == nil || l.Index(-10).val != "0" {
		t.Log("wrong result of List.Index()")
		t.Fail()
	}
	if l.Index(5) == nil || l.Index(5).val != "5" {
		t.Log("wrong result of List.Index()")
		t.Fail()
	}
	

	// test range
	range1 := l.Range(3, 5)
	if len(range1) != 3 {
		t.Fail()
	}
	for i := 0; i < len(range1); i++ {
		if range1[i].val != strconv.Itoa(i+3) {
			t.Fail()
		}
	}
	range1 = l.Range(-10, 123)
	if len(range1) != 10 {
		t.Fail()
	}
	for i := 0; i < len(range1); i++ {
		if range1[i].val != strconv.Itoa(i) {
			t.Fail()
		}
	}
	range1 = l.Range(-123, -11)
	if range1 != nil {
		t.Fail()
	}
	range1 = l.Range(10, 123)
	if range1 != nil {
		t.Fail()
	}
	range1 = l.Range(6, 1)
	if range1 != nil {
		t.Fail()
	}

	// test pop
	l.PopHead()
	l.PopTail()
	range1 = l.Range(-123, 123)
	if len(range1) != 8 {
		t.Fail()
	}
	for i := 0; i < len(range1); i++ {
		if range1[i].val != strconv.Itoa(i+1) {
			t.Fail()
		}
	}
}