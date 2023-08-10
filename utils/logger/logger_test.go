package logger

import (
	"os"
	"testing"
	"bufio"
	"strings"
	"log"
	"io"
)

func TestInfo(t *testing.T) {
	path := "info.txt"
	logfile := CreateLogfile(path)
	logger = log.New(io.MultiWriter(os.Stdout, logfile), "[SIMP REDIS]", log.LstdFlags)
	a := 123
	Info("test:%d", a)
	file, err := os.Open(path)
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	index := strings.LastIndex(string(line), "[INFO]")
	if index == -1 {
		t.Log("wrong log format")
		t.FailNow()
	}
	logstr := string(line)[index+6:]
	if strings.Compare(logstr, "test:123\n") != 0 {
		t.Logf("log wrong content: %s, supposed to be test:123\n", logstr)
		t.FailNow()
	}
	os.RemoveAll(path)
}

func TestWarn(t *testing.T) {
	path := "warn.txt"
	logfile := CreateLogfile(path)
	logger = log.New(io.MultiWriter(os.Stdout, logfile), "[SIMP REDIS]", log.LstdFlags)
	a := 123
	Warn("test:%d", a)
	file, err := os.Open(path)
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	index := strings.LastIndex(string(line), "[WARN]")
	if index == -1 {
		t.Log("wrong log format")
		t.FailNow()
	}
	logstr := string(line)[index+6:]
	if strings.Compare(logstr, "test:123\n") != 0 {
		t.Logf("log wrong content: %s, supposed to be test:123\n", logstr)
		t.FailNow()
	}
	os.RemoveAll(path)
}

func TestError(t *testing.T) {
	path := "error.txt"
	logfile := CreateLogfile(path)
	logger = log.New(io.MultiWriter(os.Stdout, logfile), "[SIMP REDIS]", log.LstdFlags)
	a := 123
	Error("test:%d", a)
	file, err := os.Open(path)
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	index := strings.LastIndex(string(line), "[ERROR]")
	if index == -1 {
		t.Log("wrong log format")
		t.FailNow()
	}
	logstr := string(line)[index+7:]
	if strings.Compare(logstr, "test:123\n") != 0 {
		t.Logf("log wrong content: %s, supposed to be test:123\n", logstr)
		t.FailNow()
	}
	os.RemoveAll(path)
}