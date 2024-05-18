// Copyright 2016 The C2KV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"github.com/Mulily0513/C2KV/client/pkg/logutil"
	"log"
	"os"

	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zapgrpc"
	"google.golang.org/grpc/grpclog"
)

func init() {
	// We override grpc logger only when the environment variable is set
	// in order to not interfere by default with user's code or other libraries.
	if os.Getenv("C2KV_CLIENT_DEBUG") != "" {
		lg, err := logutil.CreateDefaultZapLogger(C2KVClientDebugLevel())
		if err != nil {
			panic(err)
		}
		lg = lg.Named("C2KV-client")
		grpclog.SetLoggerV2(zapgrpc.NewLogger(lg))
	}
}

// SetLogger sets grpc logger.
//
// Deprecated: use grpclog.SetLoggerV2 directly or grpc_zap.ReplaceGrpcLoggerV2.
func SetLogger(l grpclog.LoggerV2) {
	grpclog.SetLoggerV2(l)
}

// C2KVClientDebugLevel translates C2KV_CLIENT_DEBUG into zap log level.
func C2KVClientDebugLevel() zapcore.Level {
	envLevel := os.Getenv("C2KV_CLIENT_DEBUG")
	if envLevel == "" || envLevel == "true" {
		return zapcore.InfoLevel
	}
	var l zapcore.Level
	if err := l.Set(envLevel); err != nil {
		log.Printf("Invalid value for environment variable 'C2KV_CLIENT_DEBUG'. Using default level: 'info'")
		return zapcore.InfoLevel
	}
	return l
}
