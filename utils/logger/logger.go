package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"simpredis/utils/config"
	"time"
	"sync"
)

var logger *log.Logger
var once sync.Once

func FileExists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		return os.IsExist(err)
	}
	return true
}

func CreateLogfile(logpath string) *os.File {
	if logpath == "" {
		logdir := config.Cfg.Logdir
		if !FileExists(logdir) {
			err := os.Mkdir(logdir, 0777)
			if err != nil {
				panic(fmt.Sprintf("Can't create log dir, err: %v", err))
			}
		}
		logpath = logdir + "/" + time.Now().Format("2006-01-02 15:04") + ".txt"
	}
	logFile, err := os.Create(logpath)
	if err != nil {
		panic(fmt.Sprintf("Can't create log file, err: %v", err))
	}
	return logFile
}

func Info(format string, a ...any) {
	logger.Printf("[INFO]"+format, a...)
}

func Warn(format string, a ...any) {
	logger.Printf("[WARN]"+format, a...)
}

func Error(format string, a ...any) {
	logger.Printf("[ERROR]"+format, a...)
}

func Init() {
	once.Do(func() {
		logfile := CreateLogfile("")
		logger = log.New(io.MultiWriter(os.Stdout, logfile), "[SIMP REDIS]", log.LstdFlags)
	})
}