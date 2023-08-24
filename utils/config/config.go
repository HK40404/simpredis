package config

import (
	"bufio"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
)

type Config struct {
	Bind       string `cfg:"bind"`
	Port       string `cfg:"port"`
	Logdir     string `cfg:"logdir"`
	ShardCount string `cfg:"shardcount"`
}

// 提供默认配置，应对无配置文件的情况
var Cfg = &Config{Bind: "0.0.0.0", Port: "7000", Logdir: "logs", ShardCount: "16"}

// 自动parse
func parse(file io.Reader) error {
	cfgmap := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.Trim(line, " ")
		// 空行或者注释，直接跳过
		if len(line) <= 0 || line[0] == '#' {
			continue
		}
		pivot := strings.IndexAny(line, " ")
		if pivot == -1 {
			return errors.New("invalid configure format")
		}
		key := line[:pivot]
		value := strings.TrimLeft(line[pivot+1:], " ")
		cfgmap[key] = value
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	t := reflect.TypeOf(Cfg)
	v := reflect.ValueOf(Cfg)

	for i := 0; i < t.Elem().NumField(); i++ {
		filed := t.Elem().Field(i)
		filedVal := v.Elem().Field(i)
		key, ok := filed.Tag.Lookup("cfg")
		if !ok {
			key = filed.Name
		}
		if v, ok := cfgmap[key]; ok {
			filedVal.SetString(v)
		}
	}
	return nil
}

func LoadConfig(cfgpath string) error {
	cfgFile, err := os.Open(cfgpath)
	if err != nil {
		return err
	}
	return parse(cfgFile)
}
