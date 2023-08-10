package parser

const (
	CRLF = "\r\n"
	EmptyBulkString = "$-1"+CRLF
)

func MakeOKReply() *String {
	return NewString("OK")
}

func MakeNullBulkReply() *BulkString {
	return NewBulkString(nil)
}