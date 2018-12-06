package main

import (
	"flag"
	"os"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	_ "google.golang.org/grpc/encoding/gzip" // get gzip compressor registered

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/flagext"
	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/common/tracing"
)

func main() {
	var (
		serverConfig server.Config
		limits       validation.Limits
	)

	flagext.RegisterFlags(&serverConfig, &limits)
	flag.Parse()

	// Setting the environment variable JAEGER_AGENT_HOST enables tracing
	trace := tracing.NewFromEnv("overrides-exporter")
	defer trace.Close()

	util.InitLogger(&serverConfig)

	server, err := server.New(serverConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing server", "err", err)
		os.Exit(1)
	}
	defer server.Shutdown()

	overrides, err := validation.NewOverrides(limits)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing overrides struct", "err", err)
		os.Exit(1)
	}

	prometheus.MustRegister(overrides)
	server.Run()
}
