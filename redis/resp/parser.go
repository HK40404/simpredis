package parser

import (
	"bufio"
	"bytes"
	"io"
	"strconv"
)


type Payload struct {
	Data RespData
	Err error
}

// 从reader中解析数据流成resp命令
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
} 

func parse0(rawreader io.Reader, ch chan<- *Payload) {
	reader := bufio.NewReader(rawreader)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			ch <- &Payload{ Err: err }
			return
		}
		// 判断是否为空行或者不以CRLF结尾（不符合RESP）
		if (len(line) <= 2 || line[len(line)-2] != '\r') {
			// 直接忽略这一行
			continue
		}
		line = bytes.TrimSuffix(line, []byte(CRLF))
		switch(line[0]) {
		case StringBegin:
			ch <- &Payload {
				Data: NewString(string(line[1:])),
			}
		case ErrorBegin:
			ch <- &Payload{
				Data: NewError(string(line[1:])),
			}
		case IntegerBegin:
			num, err := strconv.ParseInt(string(line[1:]), 10, 64)
			if err != nil {
				SendProtocolError(ch, "invalid integer format")
				continue
			}
			ch <- &Payload{
				Data: NewInteger(num),
			}
		case BulkStringBegin:
			// 只返回系统级别的错误
			err := parseBulkString(reader, string(line[1:]), ch)
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
		case ArrayBegin:
			err := parseArray(reader, string(line[1:]), ch)
			if err != nil {
				ch <- &Payload{Err: err}
				close(ch)
				return
			}
		default:
			continue
		}
	}
}

func parseBulkString(reader *bufio.Reader, line string, ch chan<- *Payload) error {
	byteCount, err := strconv.Atoi(line)
	if err != nil || byteCount < -1 {
		SendProtocolError(ch, "invalid bulkstring length")
		return nil
	} else if byteCount == -1 {
		ch <- &Payload{ Data: NewBulkString(nil) }
		return nil
	}

	buf := make([]byte, byteCount+len(CRLF))
	_, err = io.ReadFull(reader, buf)
	if err != nil {
		return err
	}
	ch <- &Payload{ Data: NewBulkString(buf[:len(buf)-2]) }
	return nil
}

func parseArray(reader *bufio.Reader, header string, ch chan<- *Payload) error {
	strCount, err := strconv.Atoi(header)
	if err != nil || strCount < 0 {
		SendProtocolError(ch, "invalid bulkstring count of array")
		return nil
	}
	
	array := make([][]byte, strCount)
	for i := 0; i < strCount; i++ {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		// 判断是否为空行或者不以CRLF结尾（不符合RESP）
		if (len(line) <= 2 || line[len(line)-2] != '\r' || line[0] != BulkStringBegin) {
			// 直接忽略这一行 
			SendProtocolError(ch, "invalid bulkstring format")
			return nil
		}
		line = bytes.TrimSuffix(line[1:], []byte(CRLF))
		strLen, err := strconv.Atoi(string(line))
		if err != nil || strLen < -1 {
			SendProtocolError(ch, "invalid bulkstring length")
			return nil
		} else if strLen == -1 {
			// NullBulkString后面没有body
			array[i] = nil
			continue
		}

		buf := make([]byte, strLen+len(CRLF))
		_, err = io.ReadFull(reader, buf)
		if err != nil {
			return err
		}
		array[i] = buf[:len(buf)-2]
	}
	ch <- &Payload{ Data: NewArray(array)}
	return nil
}