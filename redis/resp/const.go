package parser

const (
	CRLF = "\r\n"
	EmptyBulkString = "$-1"+CRLF
	PING = "PING"
)

func MakeOKReply() *String {
	return NewString("OK")
}

func MakeNullBulkReply() *BulkString {
	return NewBulkString(nil)
}