package client

import "strings"

func LineToArgs(line string) [][]byte {
	line = strings.Trim(line, " ")
	argstrs := strings.Split(line, " ")
	args := make([][]byte, 0, len(argstrs))
	for _, s := range argstrs {
		args = append(args, []byte(s))
	}
	return args
}
