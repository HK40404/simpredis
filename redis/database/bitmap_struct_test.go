package database

import (
	"bytes"
	"testing"
)

func TestGetSetbit(t *testing.T) {
	bm := []byte{0}
	SetBit(&bm, 10, 1)
	if len(bm) != 2 {
		t.Fail()
	}
	t.Logf("%08b", bm)
	if GetBit(&bm, 10) != 1 {
		t.Fail()
	}
	for i := 0; i < len(bm)*8; i++ {
		if i == 10 {
			continue
		}
		if GetBit(&bm, i) != 0 {
			t.Fail()
		}
	}
}

func TestBitCount(t *testing.T) {
	bm := []byte{ 0b10010001, 0b11101110 }
	count := BitCount(&bm, 0, 4)
	if count != 2 {
		t.Log(count)
		t.Fail()
	}

	count = BitCount(&bm, 5, 15)
	if count != 7 {
		t.Log(count)
		t.Fail()
	}

	count = BitCount(&bm, 8, 8)
	if count != 0 {
		t.Log(count)
		t.Fail()
	}

	count = BitCount(&bm, 10, 14)
	if count != 4 {
		t.Log(count)
		t.Fail()
	}

	count = BitCount(&bm, 0, 15)
	if count != 9 {
		t.Log(count)
		t.Fail()
	}
}

func TestBitOp(t *testing.T) {
	vals := [][]byte{
		{ 9 },
		{ 5, 0 },
		{ 1, 0, 0x80 },
	}
	res := BitOp("and", vals)
	if !bytes.Equal(res, []byte{1,0,0}) {
		t.Fail()
	}
	res = BitOp("or", vals)
	if !bytes.Equal(res, []byte{0x0d,0,0x80}) {
		t.Fail()
	}
	res = BitOp("xor", vals)
	if !bytes.Equal(res, []byte{0x0d,0,0x80}) {
		t.Fail()
	}
	res = BitOp("not", vals[0:1])
	if !bytes.Equal(res, []byte{0b11110110}) {
		t.Fail()
	}
	vals[1] = nil
	res = BitOp("and", vals)
	if !bytes.Equal(res, []byte{0,0,0}) {
		t.Fail()
	}
	res = BitOp("or", vals)
	if !bytes.Equal(res, []byte{0x09,0,0x80}) {
		t.Fail()
	}
	res = BitOp("xor", vals)
	if !bytes.Equal(res, []byte{0x08,0,0x80}) {
		t.Fail()
	}
	res = BitOp("not", vals[1:2])
	if len(res) != 0 {
		t.Fail()
	}
}