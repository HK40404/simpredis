package parser

import (
	"bytes"
	"testing"
)

func TestRESP(t *testing.T) {
	emptyArrayByte := []byte("*0\r\n")
	if !bytes.Equal(NewArray(nil).Serialize(), emptyArrayByte) {
		t.Fail()
	}
	if !bytes.Equal(NewArray(make([][]byte, 0, 10)).Serialize(), emptyArrayByte) {
		t.Fail()
	}

	nilsArrayByte := []byte("*3\r\n$-1\r\n$-1\r\n$-1\r\n")
	nilsArray := make([][]byte, 0)
	nilsArray = append(nilsArray, nil)
	nilsArray = append(nilsArray, nil)
	nilsArray = append(nilsArray, nil)
	if !bytes.Equal(NewArray(nilsArray).Serialize(), nilsArrayByte) {
		t.Fail()
	}

	// "" and (nil)
	if bytes.Equal(NewBulkString(make([]byte, 0)).Serialize(), NewBulkString(nil).Serialize()) {
		t.Fail()
	}

	// (empty list or set)
	if !bytes.Equal(NewArray(make([][]byte, 0)).Serialize(), NewArray(nil).Serialize()) {
		t.Fail()
	}
}
