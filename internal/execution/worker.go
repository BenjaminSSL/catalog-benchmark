package execution

import (
	"benchmark/internal/common"
	"context"
	"net/http"
)

type WorkerConfig struct {
	Func    func(ctx context.Context, client *http.Client, logger *common.RoutineBatchLogger)
	Threads int
}
