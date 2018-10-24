// +build linux

package singularity

import (
	"sync"
	"time"

	hclog "github.com/hashicorp/go-hclog"
	plugin "github.com/hashicorp/go-plugin"
	"github.com/hashicorp/nomad/client/driver/env"
	"github.com/hashicorp/nomad/client/driver/executor"
	"github.com/hashicorp/nomad/plugins/drivers"
)

type singularityTaskHandle struct {
	exec         executor.Executor
	env          *env.TaskEnv
	uuid         string
	pid          int
	pluginClient *plugin.Client
	logger       hclog.Logger

	// stateLock syncs access to all fields below
	stateLock sync.RWMutex

	taskConfig  *drivers.TaskConfig
	procState   drivers.TaskState
	startedAt   time.Time
	completedAt time.Time
	exitResult  *drivers.ExitResult
}

func (h *singularityTaskHandle) IsRunning() bool {
	return h.procState == drivers.TaskStateRunning
}

func (h *singularityTaskHandle) run() {

	// since run is called immediately after the handle is created this
	// ensures the exitResult is initialized so we avoid a nil pointer
	// thus it does not need to be included in the lock
	if h.exitResult == nil {
		h.exitResult = &drivers.ExitResult{}
	}

	ps, err := h.exec.Wait()
	h.stateLock.Lock()
	defer h.stateLock.Unlock()

	if err != nil {
		h.exitResult.Err = err
		h.procState = drivers.TaskStateUnknown
		h.completedAt = time.Now()
		return
	}
	h.procState = drivers.TaskStateExited
	h.exitResult.ExitCode = ps.ExitCode
	h.exitResult.Signal = ps.Signal
	h.completedAt = ps.Time

	// TODO: detect if the taskConfig OOMed
}
