package parser

import (
	"errors"
	"strconv"
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
	raws := []byte{StringBegin}
	raws = append(raws, []byte(ss.Arg)...)
	raws = append(raws, []byte(CRLF)...)
	return raws
}

type Error struct {
	Arg string
}

func NewError(data string) *Error{
	return &Error{ Arg: data }
}

func (e *Error) Serialize() []byte {
	raws := []byte{ErrorBegin}
	raws = append(raws, []byte(e.Arg)...)
	raws = append(raws, []byte(CRLF)...)
	return raws
}

type Integer struct {
	Arg int64
}

func NewInteger(num int64) *Integer {
	return &Integer{ Arg: num }
}

func (e *Integer) Serialize() []byte {
	raws := []byte{IntegerBegin}
	num := []byte(strconv.FormatInt(e.Arg, 10))
	raws = append(raws, num...)
	raws = append(raws, []byte(CRLF)...)
	return raws
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
	header := string(BulkStringBegin) + strconv.Itoa(len(bs.Arg)) + CRLF
	body := string(bs.Arg) + CRLF
	return []byte(header + body)
}

type Array struct {
	Args [][]byte
}

func NewArray(strs [][]byte) *Array {
	return &Array{ Args: strs }
}

func (array *Array) Serialize() []byte {
	res := []byte(string(ArrayBegin) + strconv.Itoa(len(array.Args)) + CRLF)
	for _, v := range array.Args {
		res = append(res, NewBulkString(v).Serialize()...)
	}
	return res
}