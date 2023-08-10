package config

import (
	"os"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "*.cfg")
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	defer os.RemoveAll(tmpfile.Name())

	cfgstr := `#这是配置文件

    bind 1.2.3.4
    port             10086
    shardcount 19
    `
	_, err = tmpfile.Write([]byte(cfgstr))
	if err != nil {
		t.Log(err)
		t.FailNow()
	}
	err = tmpfile.Sync()
	if err != nil {
		t.Log(err)
		t.FailNow()
	}

	LoadConfig(tmpfile.Name())
	if Cfg.Bind != "1.2.3.4" || Cfg.Port != "10086" || Cfg.Logdir != "logs" || Cfg.ShardCount != "19" {
		t.Logf("parsed content not match, parsed content: %+v", *Cfg)
		t.Fail()
	}
}
