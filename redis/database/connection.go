package database

import (
	parser "simpredis/redis/resp"
)

func ExecPing(engine *DBEngine, args [][]byte) parser.RespData {
	switch len(args) {
	case 1:
		return parser.NewString("PONG")
	case 2:
		return parser.NewBulkString(args[1])
	default:
		return parser.NewError("Invalid command format")
	}
}

func ExecEcho(engine *DBEngine, args [][]byte) parser.RespData {
	switch len(args) {
	case 2:
		return parser.NewBulkString(args[1])
	default:
		return parser.NewError("Invalid command format")
	}
}


func init() {
	RegisterCmd("ping", ExecPing)
	RegisterCmd("echo", ExecEcho)
}