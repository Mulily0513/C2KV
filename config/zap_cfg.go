package config

type ZapConfig struct {
	Level        string `json:"level" yaml:"level"`
	Prefix       string `json:"prefix" yaml:"prefix"`
	Format       string `json:"format" yaml:"format"` // Log output format. JSON format is convenient for machine processing, and console format is convenient for reading.
	Director     string `json:"director"  yaml:"director"`
	EncodeLevel  string `json:"encodeLevel" yaml:"encodeLevel"`
	MaxAge       int    `json:"maxAge" yaml:"maxAge"`
	ShowLine     bool   `json:"showLine" yaml:"showLine"`
	LogInConsole bool   `json:"logInConsole" yaml:"logInConsole"`
}
