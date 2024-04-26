package log

import (
	"github.com/Mulily0513/C2KV/config"
	"testing"
)

func TestInitLog(t *testing.T) {
	cfg := &config.ZapConfig{
		Level:         "debug",
		Format:        "console",
		Prefix:        "[Cold2DB]",
		Director:      "./log",
		ShowLine:      true,
		EncodeLevel:   "LowercaseColorLevelEncoder",
		StacktraceKey: "stacktrace",
		LogInConsole:  true,
	}
	InitLog(cfg)
	Debugf("测试")
}
