package config

type ZapConfig struct {
	Level        string `json:"level" yaml:"level"`                 // 级别
	Prefix       string `json:"prefix" yaml:"prefix"`               // 日志前缀
	Format       string `json:"format" yaml:"format"`               // 日志输出格式 json格式便于机器处理,console格式便于阅读
	Director     string `json:"director"  yaml:"director"`          // 日志文件夹
	EncodeLevel  string `json:"encodeLevel" yaml:"encode-level"`    // 编码级
	MaxAge       int    `json:"maxAge" yaml:"max-age"`              // 日志留存时间单位天
	ShowLine     bool   `json:"showLine" yaml:"show-line"`          // 显示行
	LogInConsole bool   `json:"logInConsole" yaml:"log-in-console"` // 输出控制台
}
