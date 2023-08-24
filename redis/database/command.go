package database

import (
	parser "github.com/HK40404/simpredis/redis/resp"
	"github.com/HK40404/simpredis/utils/logger"
)

var CmdTable = make(map[string]CmdFuc)

type CmdFuc func(db *DBEngine, array [][]byte) parser.RespData

func RegisterCmd(cmd string, fun CmdFuc) {
	if _, ok := CmdTable[cmd]; ok {
		logger.Error("this cmd has been registerd!")
		return
	}
	CmdTable[cmd] = fun
}
