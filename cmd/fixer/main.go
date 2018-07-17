package main

import (
	"context"
	"flag"
	"os"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/cortex/pkg/chunk"
	"github.com/weaveworks/cortex/pkg/chunk/storage"
	"github.com/weaveworks/cortex/pkg/util"
)

func main() {
	var (
		schemaConfig  chunk.SchemaConfig
		storageConfig storage.Config
		serverConfig  server.Config
		userID        string
		from, through int64
		limit         int
	)
	util.RegisterFlags(&schemaConfig, &storageConfig, &serverConfig)
	flag.StringVar(&userID, "userid", "", "")
	flag.Int64Var(&from, "from", 0, "")
	flag.Int64Var(&through, "through", 0, "")
	flag.IntVar(&limit, "limit", 10000, "")
	flag.Parse()

	util.InitLogger(&serverConfig)

	storageClient, err := storage.NewStorageClient(storageConfig, schemaConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing storage client", "err", err)
		os.Exit(1)
	}

	fixer, err := chunk.NewFixer(storageClient, schemaConfig)
	if err != nil {
		level.Error(util.Logger).Log("msg", "error initializing fixer", "err", err)
		os.Exit(1)
	}

	fixer.Fix(context.Background(), userID, model.Time(from), model.Time(through), limit)
}
