package client

import (
	"testing"
)

func TestLineToArgs(t *testing.T) {
	cmd := "   lpush key value1 value2            "
	args := LineToArgs(cmd)
	if len(args) != 4 {
		t.Log(len(args))
		t.Fail()
	}
	if string(args[0]) != "lpush" {
		t.Fail()
	}
	if string(args[1]) != "key" {
		t.Fail()
	}
	if string(args[2]) != "value1" {
		t.Fail()
	}
	if string(args[3]) != "value2" {
		t.Fail()
	}
}