package parser

import (
	"errors"
	"strconv"
	"bytes"
)

const (
	StringBegin = '+'
	ErrorBegin = '-'
	IntegerBegin = ':'
	BulkStringBegin = '$'
	ArrayBegin = '*'
)

type RespData interface {
	Serialize() []byte
}

type String struct{
	Arg string
}

func NewString(data string) *String {
	return &String{ Arg: data }
}

func (ss *String) Serialize() []byte {
	return []byte("+" + ss.Arg + CRLF)
}

type Error struct {
	Arg string
}

func NewError(data string) *Error{
	return &Error{ Arg: data }
}

func (e *Error) Serialize() []byte {
	return []byte("-" + e.Arg + CRLF)
}

type Integer struct {
	Arg int64
}

func NewInteger(num int64) *Integer {
	return &Integer{ Arg: num }
}

func (e *Integer) Serialize() []byte {
	return []byte(":" + strconv.FormatInt(e.Arg, 10) + CRLF)
}

func SendProtocolError(ch chan<- *Payload, msg string) {
	err := errors.New("Invalid Protocol Syntax: " + msg)
	ch <- &Payload{Err: err}
}


type BulkString struct {
	Arg []byte
}

func NewBulkString(data []byte) *BulkString {
	return &BulkString{ Arg: data}
}

func (bs *BulkString) Serialize() []byte {
	if bs.Arg == nil {
		return []byte(EmptyBulkString)
	}
	return []byte(string(BulkStringBegin) + strconv.Itoa(len(bs.Arg)) + CRLF + string(bs.Arg) + CRLF)
}

type Array struct {
	Args [][]byte
}

func NewArray(strs [][]byte) *Array {
	return &Array{ Args: strs }
}

func (array *Array) Serialize() []byte {
	argLen := len(array.Args)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range array.Args {
		if arg == nil {
			buf.WriteString("$-1" + CRLF)
		} else {
			buf.WriteString("$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF)
		}
	}
	return buf.Bytes()
}