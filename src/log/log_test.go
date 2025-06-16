package log

import (
	"github.com/Mulily0513/C2KV/src/config"
	"testing"
)

func TestInitLog(t *testing.T) {
	cfg := &config.ZapConfig{
		Level:        "debug",
		Format:       "console",
		Prefix:       "[C2KV]",
		Director:     "./log",
		ShowLine:     true,
		EncodeLevel:  "LowercaseColorLevelEncoder",
		LogInConsole: true,
	}
	InitLog(cfg)
	Debugf("测试")
}
