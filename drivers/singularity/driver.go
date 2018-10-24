// +build linux

package singularity

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	appcschema "github.com/appc/spec/schema"
	"github.com/hashicorp/consul-template/signals"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/hashicorp/nomad/client/config"
	"github.com/hashicorp/nomad/client/driver/env"
	"github.com/hashicorp/nomad/client/driver/executor"
	dstructs "github.com/hashicorp/nomad/client/driver/structs"
	cstructs "github.com/hashicorp/nomad/client/structs"
	"github.com/hashicorp/nomad/drivers/shared/eventer"
	"github.com/hashicorp/nomad/plugins/base"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/hashicorp/nomad/plugins/drivers/utils"
	"github.com/hashicorp/nomad/plugins/shared/hclspec"
	"github.com/hashicorp/nomad/plugins/shared/loader"

	"golang.org/x/net/context"
)

const (
	// pluginName is the name of the plugin
	pluginName = "singularity"

	// fingerprintPeriod is the interval at which the driver will send fingerprint responses
	fingerprintPeriod = 30 * time.Second

	// singularityVersion is the earliest supported version of singularity
	singularityVersion = "v3.0.0"

	// singularityCmd is the command singularity is installed as.
	singularityCmd = "singularity"
)

var (
	// PluginID is the singularity plugin metadata registered in the plugin
	// catalog.
	PluginID = loader.PluginID{
		Name:       pluginName,
		PluginType: base.PluginTypeDriver,
	}

	// PluginConfig is the singularity factory function registered in the
	// plugin catalog.
	PluginConfig = &loader.InternalPluginConfig{
		Config:  map[string]interface{}{},
		Factory: func(l hclog.Logger) interface{} { return NewsingularityDriver(l) },
	}
)

func PluginLoader(opts map[string]string) (map[string]interface{}, error) {
	conf := map[string]interface{}{}

	return conf, nil
}

var (
	// pluginInfo is the response returned for the PluginInfo RPC
	pluginInfo = &base.PluginInfoResponse{
		Type:             base.PluginTypeDriver,
		PluginApiVersion: "0.0.1",
		PluginVersion:    "0.1.0",
		Name:             pluginName,
	}

	// configSpec is the hcl specification returned by the ConfigSchema RPC
	configSpec = hclspec.NewObject(map[string]*hclspec.Spec{
		"volumes_enabled": hclspec.NewDefault(
			hclspec.NewAttr("volumes_enabled", "bool", false),
			hclspec.NewLiteral("true"),
		),
	})

	// taskConfigSpec is the hcl specification for the driver config section of
	// a taskConfig within a job. It is returned in the TaskConfigSchema RPC
	taskConfigSpec = hclspec.NewObject(map[string]*hclspec.Spec{
		"image":   hclspec.NewAttr("image", "string", true),
		"command": hclspec.NewAttr("command", "string", false),
		"args":    hclspec.NewAttr("args", "list(string)", false),

		"binds":   hclspec.NewAttr("bind", "list(string)", false),
		"contain": hclspec.NewAttr("contain", "bool", false),
		"home":    hclspec.NewAttr("home", "string", false),
		"workdir": hclspec.NewAttr("workdir", "string", false),
		"pwd":     hclspec.NewAttr("pwd", "string", false),
		"debug":   hclspec.NewAttr("debug", "bool", false),
	})

	// capabilities is returned by the Capabilities RPC and indicates what
	// optional features this driver supports
	capabilities = &drivers.Capabilities{
		SendSignals: true,
		Exec:        false,
		FSIsolation: cstructs.FSIsolationChroot,
	}
)

// Config is the client configuration for the driver
type Config struct {
	// VolumesEnabled allows tasks to bind host paths (volumes) inside their
	// container. Binding relative paths is always allowed and will be resolved
	// relative to the allocation's directory.
	VolumesEnabled bool `codec:"volumes_enabled"`
}

// TaskConfig is the driver configuration of a taskConfig within a job
type TaskConfig struct {
	ImageName string   `codec:"image"`
	Command   string   `codec:"command"`
	Args      []string `codec:"args"`

	Binds   []string `codec:"binds"` // Host-Volumes to mount in, syntax: /path/to/host/directory:/destination/path/in/container
	Contain bool     `codec:"contain"`
	Home    string   `codec:"home"`
	Workdir string   `codec:"workdir"`
	Pwd     string   `codec:"pwd"`
	Debug   bool     `codec:"debug"` // Enable debug option for singularity command
}

// singularityTaskState is the state which is encoded in the handle returned in
// StartTask. This information is needed to rebuild the taskConfig state and handler
// during recovery.
type singularityTaskState struct {
	ReattachConfig *utils.ReattachConfig
	TaskConfig     *drivers.TaskConfig
	Pid            int
	StartedAt      time.Time
	UUID           string
}

// singularityDriver is a driver for running images via singularity
// We attempt to chose sane defaults for now, with more configuration available
// planned in the future
type singularityDriver struct {
	// eventer is used to handle multiplexing of TaskEvents calls such that an
	// event can be broadcast to all callers
	eventer *eventer.Eventer

	// config is the driver configuration set by the SetConfig RPC
	config *Config

	// tasks is the in memory datastore mapping taskIDs to singularityTaskHandles
	tasks *taskStore

	// ctx is the context for the driver. It is passed to other subsystems to
	// coordinate shutdown
	ctx context.Context

	// signalShutdown is called when the driver is shutting down and cancels the
	// ctx passed to any subsystems
	signalShutdown context.CancelFunc

	// logger will log to the plugin output which is usually an 'executor.out'
	// file located in the root of the TaskDir
	logger hclog.Logger
}

func NewsingularityDriver(logger hclog.Logger) drivers.DriverPlugin {
	ctx, cancel := context.WithCancel(context.Background())
	logger = logger.Named(pluginName)
	return &singularityDriver{
		eventer:        eventer.NewEventer(ctx, logger),
		config:         &Config{},
		tasks:          newTaskStore(),
		ctx:            ctx,
		signalShutdown: cancel,
		logger:         logger,
	}
}

func (d *singularityDriver) PluginInfo() (*base.PluginInfoResponse, error) {
	return pluginInfo, nil
}

func (d *singularityDriver) ConfigSchema() (*hclspec.Spec, error) {
	return configSpec, nil
}

func (d *singularityDriver) SetConfig(data []byte) error {
	var config Config
	if err := base.MsgPackDecode(data, &config); err != nil {
		return err
	}

	d.config = &config
	return nil
}

func (d *singularityDriver) TaskConfigSchema() (*hclspec.Spec, error) {
	return taskConfigSpec, nil
}

func (d *singularityDriver) Capabilities() (*drivers.Capabilities, error) {
	return capabilities, nil
}

func (r *singularityDriver) Fingerprint(ctx context.Context) (<-chan *drivers.Fingerprint, error) {
	ch := make(chan *drivers.Fingerprint)
	go r.handleFingerprint(ctx, ch)
	return ch, nil
}

func (d *singularityDriver) handleFingerprint(ctx context.Context, ch chan *drivers.Fingerprint) {
	defer close(ch)
	ticker := time.NewTimer(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			ticker.Reset(fingerprintPeriod)
			ch <- d.buildFingerprint()
		}
	}
}

func (d *singularityDriver) buildFingerprint() *drivers.Fingerprint {
	fingerprint := &drivers.Fingerprint{
		Attributes:        map[string]string{},
		Health:            drivers.HealthStateHealthy,
		HealthDescription: "healthy",
	}

	_, err := exec.Command(singularityCmd, "version").Output()
	if err != nil {
		fingerprint.Health = drivers.HealthStateUndetected
		fingerprint.HealthDescription = fmt.Sprintf("failed to executor %s version: %v", singularityCmd, err)
		return fingerprint
	}

	fingerprint.Attributes["driver.singularity"] = "1"
	fingerprint.Attributes["driver.singularity.version"] = "v3.0.0"
	if d.config.VolumesEnabled {
		fingerprint.Attributes["driver.singularity.volumes.enabled"] = "1"
	}

	return fingerprint

}

func (d *singularityDriver) RecoverTask(handle *drivers.TaskHandle) error {
	if handle == nil {
		return fmt.Errorf("error: handle cannot be nil")
	}

	var taskState singularityTaskState
	if err := handle.GetDriverState(&taskState); err != nil {
		d.logger.Error("failed to decode taskConfig state from handle", "error", err, "task_id", handle.Config.ID)
		return fmt.Errorf("failed to decode taskConfig state from handle: %v", err)
	}

	plugRC, err := utils.ReattachConfigToGoPlugin(taskState.ReattachConfig)
	if err != nil {
		d.logger.Error("failed to build ReattachConfig from taskConfig state", "error", err, "task_id", handle.Config.ID)
		return fmt.Errorf("failed to build ReattachConfig from taskConfig state: %v", err)
	}

	pluginConfig := &plugin.ClientConfig{
		Reattach: plugRC,
	}

	execImpl, pluginClient, err := utils.CreateExecutorWithConfig(pluginConfig, os.Stderr)
	if err != nil {
		d.logger.Error("failed to reattach to executor", "error", err, "task_id", handle.Config.ID)
		return fmt.Errorf("failed to reattach to executor: %v", err)
	}

	// The taskConfig's environment is set via --set-env flags in Start, but the singularity
	// command itself needs an environment with PATH set to find iptables.
	// TODO (preetha) need to figure out how to read env.blacklist
	eb := env.NewEmptyBuilder()
	filter := strings.Split(config.DefaultEnvBlacklist, ",")
	singularityEnv := eb.SetHostEnvvars(filter).Build()

	h := &singularityTaskHandle{
		exec:         execImpl,
		env:          singularityEnv,
		pid:          taskState.Pid,
		uuid:         taskState.UUID,
		pluginClient: pluginClient,
		taskConfig:   taskState.TaskConfig,
		procState:    drivers.TaskStateRunning,
		startedAt:    taskState.StartedAt,
		exitResult:   &drivers.ExitResult{},
	}

	d.tasks.Set(taskState.TaskConfig.ID, h)

	go h.run()
	return nil
}

// imageExec can be used to run/exec/shell a Singularity image
// it return the exitCode and err of the execution
func imageExec(action string, opts TaskConfig, imagePath string, command []string) (stdout string, stderr string, exitCode int, err error) {
	// action can be run/exec/shell
	argv := []string{action}
	for _, bind := range opts.Binds {
		argv = append(argv, "--bind", bind)
	}
	if opts.Contain {
		argv = append(argv, "--contain")
	}
	if opts.Home != "" {
		argv = append(argv, "--home", opts.Home)
	}
	if opts.Workdir != "" {
		argv = append(argv, "--workdir", opts.Workdir)
	}
	if opts.Pwd != "" {
		argv = append(argv, "--pwd", opts.Pwd)
	}
	argv = append(argv, imagePath)
	argv = append(argv, command...)

	var outbuf, errbuf bytes.Buffer
	cmd := exec.Command(singularityCmd, opts.Args...)

	cmd.Stdout = &outbuf
	cmd.Stderr = &errbuf

	if err := cmd.Start(); err != nil {
		log.Fatalf("cmd.Start: %v", err)
	}

	// retrieve exit code
	if err := cmd.Wait(); err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			// The program has exited with an exit code != 0
			exitCode = 1
		}
	}

	stdout = outbuf.String()
	stderr = errbuf.String()

	return
}

func (d *singularityDriver) StartTask(cfg *drivers.TaskConfig) (*drivers.TaskHandle, *cstructs.DriverNetwork, error) {
	if _, ok := d.tasks.Get(cfg.ID); ok {
		return nil, nil, fmt.Errorf("taskConfig with ID '%s' already started", cfg.ID)
	}

	var driverConfig TaskConfig

	if err := cfg.DecodeDriverConfig(&driverConfig); err != nil {
		return nil, nil, fmt.Errorf("failed to decode driver config: %v", err)
	}

	handle := drivers.NewTaskHandle(pluginName)
	handle.Config = cfg

	runArgs := make([]string, 0, 50)
	// Add debug option to singularity command.
	if driverConfig.Debug {
		runArgs = append(runArgs, "-d")
	}
	runArgs = append(runArgs, driverConfig.Command)

	for _, bind := range driverConfig.Binds {
		runArgs = append(runArgs, "--bind", bind)
	}
	if driverConfig.Contain {
		runArgs = append(runArgs, "--contain")
	}
	if driverConfig.Home != "" {
		runArgs = append(runArgs, "--home", driverConfig.Home)
	}
	if driverConfig.Workdir != "" {
		runArgs = append(runArgs, "--workdir", driverConfig.Workdir)
	}
	if driverConfig.Pwd != "" {
		runArgs = append(runArgs, "--pwd", driverConfig.Pwd)
	}
	//  image
	runArgs = append(runArgs, driverConfig.ImageName)
	runArgs = append(runArgs, driverConfig.Args...)

	pluginLogFile := filepath.Join(cfg.TaskDir().Dir, fmt.Sprintf("%s-executor.out", cfg.Name))
	executorConfig := &dstructs.ExecutorConfig{
		LogFile:  pluginLogFile,
		LogLevel: "debug",
	}

	// TODO: best way to pass port ranges in from client config
	execImpl, pluginClient, err := utils.CreateExecutor(os.Stderr, hclog.Debug, 14000, 14512, executorConfig)
	if err != nil {
		return nil, nil, err
	}

	absPath, err := GetAbsolutePath(singularityCmd)
	if err != nil {
		return nil, nil, err
	}

	// TODO (preetha) need to figure out how to pass env.blacklist from client config
	eb := env.NewEmptyBuilder()
	filter := strings.Split(config.DefaultEnvBlacklist, ",")
	singularityEnv := eb.SetHostEnvvars(filter).Build()

	// Enable ResourceLimits to place the executor in a parent cgroup of
	// the singularity container. This allows stats collection via the executor to
	// work just like it does for exec.
	execCmd := &executor.ExecCommand{
		Cmd:            absPath,
		Args:           runArgs,
		ResourceLimits: false,
		Resources: &executor.Resources{
			CPU:      int(cfg.Resources.LinuxResources.CPUShares),
			MemoryMB: int(drivers.BytesToMB(cfg.Resources.LinuxResources.MemoryLimitBytes)),
			DiskMB:   cfg.Resources.NomadResources.DiskMB,
		},
		Env:        cfg.EnvList(),
		TaskDir:    cfg.TaskDir().Dir,
		StdoutPath: cfg.StdoutPath,
		StderrPath: cfg.StderrPath,
	}
	ps, err := execImpl.Launch(execCmd)
	if err != nil {
		pluginClient.Kill()
		return nil, nil, err
	}

	d.logger.Debug("started taskConfig", "task_name", cfg.Name, "args", runArgs)
	h := &singularityTaskHandle{
		exec:         execImpl,
		env:          singularityEnv,
		pid:          ps.Pid,
		pluginClient: pluginClient,
		taskConfig:   cfg,
		procState:    drivers.TaskStateRunning,
		startedAt:    time.Now().Round(time.Millisecond),
		logger:       d.logger,
	}

	singularityDriverState := singularityTaskState{
		ReattachConfig: utils.ReattachConfigFromGoPlugin(pluginClient.ReattachConfig()),
		Pid:            ps.Pid,
		TaskConfig:     cfg,
		StartedAt:      h.startedAt,
	}

	if err := handle.SetDriverState(&singularityDriverState); err != nil {
		d.logger.Error("failed to start task, error setting driver state", "error", err, "task_name", cfg.Name)
		execImpl.Shutdown("", 0)
		pluginClient.Kill()
		return nil, nil, fmt.Errorf("failed to set driver state: %v", err)
	}

	d.tasks.Set(cfg.ID, h)
	go h.run()

	return handle, nil, nil

}

func (d *singularityDriver) WaitTask(ctx context.Context, taskID string) (<-chan *drivers.ExitResult, error) {
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return nil, drivers.ErrTaskNotFound
	}

	ch := make(chan *drivers.ExitResult)
	go d.handleWait(ctx, handle, ch)

	return ch, nil
}

func (d *singularityDriver) StopTask(taskID string, timeout time.Duration, signal string) error {
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return drivers.ErrTaskNotFound
	}

	if err := handle.exec.Shutdown(signal, timeout); err != nil {
		if handle.pluginClient.Exited() {
			return nil
		}
		return fmt.Errorf("executor Shutdown failed: %v", err)
	}

	return nil
}

func (d *singularityDriver) DestroyTask(taskID string, force bool) error {
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return drivers.ErrTaskNotFound
	}

	if handle.IsRunning() && !force {
		return fmt.Errorf("cannot destroy running task")
	}

	if !handle.pluginClient.Exited() {
		if handle.IsRunning() {
			if err := handle.exec.Shutdown("", 0); err != nil {
				handle.logger.Error("destroying executor failed", "err", err)
			}
		}

		handle.pluginClient.Kill()
	}

	d.tasks.Delete(taskID)
	return nil
}

func (d *singularityDriver) InspectTask(taskID string) (*drivers.TaskStatus, error) {
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return nil, drivers.ErrTaskNotFound
	}

	handle.stateLock.RLock()
	defer handle.stateLock.RUnlock()

	status := &drivers.TaskStatus{
		ID:          handle.taskConfig.ID,
		Name:        handle.taskConfig.Name,
		State:       handle.procState,
		StartedAt:   handle.startedAt,
		CompletedAt: handle.completedAt,
		ExitResult:  handle.exitResult,
		DriverAttributes: map[string]string{
			"pid": strconv.Itoa(handle.pid),
		},
	}

	return status, nil
}

func (d *singularityDriver) TaskStats(taskID string) (*cstructs.TaskResourceUsage, error) {
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return nil, drivers.ErrTaskNotFound
	}

	return handle.exec.Stats()
}

func (d *singularityDriver) TaskEvents(ctx context.Context) (<-chan *drivers.TaskEvent, error) {
	return d.eventer.TaskEvents(ctx)
}

func (d *singularityDriver) SignalTask(taskID string, signal string) error {
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return drivers.ErrTaskNotFound
	}

	sig := os.Interrupt
	if s, ok := signals.SignalLookup[signal]; ok {
		d.logger.Warn("signal to send to task unknown, using SIGINT", "signal", signal, "task_id", handle.taskConfig.ID, "task_name", handle.taskConfig.Name)
		sig = s
	}
	return handle.exec.Signal(sig)
}

func (d *singularityDriver) ExecTask(taskID string, cmdArgs []string, timeout time.Duration) (*drivers.ExecTaskResult, error) {
	return nil, fmt.Errorf("Singularity driver can't execute commands")
}

// GetAbsolutePath returns the absolute path of the passed binary by resolving
// it in the path and following symlinks.
func GetAbsolutePath(bin string) (string, error) {
	lp, err := exec.LookPath(bin)
	if err != nil {
		return "", fmt.Errorf("failed to resolve path to %q executable: %v", bin, err)
	}

	return filepath.EvalSymlinks(lp)
}

// Given a singularity/appc pod manifest and driver portmap configuration, create
// a driver portmap.
func singularityManifestMakePortMap(manifest *appcschema.PodManifest, configPortMap map[string]string) (map[string]int, error) {
	if len(manifest.Apps) == 0 {
		return nil, fmt.Errorf("manifest has no apps")
	}
	if len(manifest.Apps) != 1 {
		return nil, fmt.Errorf("manifest has multiple apps!")
	}
	app := manifest.Apps[0]
	if app.App == nil {
		return nil, fmt.Errorf("specified app has no App object")
	}

	portMap := make(map[string]int)
	for svc, name := range configPortMap {
		for _, port := range app.App.Ports {
			if port.Name.String() == name {
				portMap[svc] = int(port.Port)
			}
		}
	}
	return portMap, nil
}

// Create a time with a 0 to 100ms jitter for singularityGetDriverNetwork retries
func getJitteredNetworkRetryTime() time.Duration {
	return time.Duration(900+rand.Intn(100)) * time.Millisecond
}

// Conditionally elide a buffer to an arbitrary length
func elideToLen(inBuf bytes.Buffer, length int) bytes.Buffer {
	if inBuf.Len() > length {
		inBuf.Truncate(length)
		inBuf.WriteString("...")
	}
	return inBuf
}

// Conditionally elide a buffer to an 80 character string
func elide(inBuf bytes.Buffer) string {
	tempBuf := elideToLen(inBuf, 80)
	return tempBuf.String()
}

func (d *singularityDriver) handleWait(ctx context.Context, handle *singularityTaskHandle, ch chan *drivers.ExitResult) {
	defer close(ch)
	var result *drivers.ExitResult
	ps, err := handle.exec.Wait()
	if err != nil {
		result = &drivers.ExitResult{
			Err: fmt.Errorf("executor: error waiting on process: %v", err),
		}
	} else {
		result = &drivers.ExitResult{
			ExitCode: ps.ExitCode,
			Signal:   ps.Signal,
		}
	}

	select {
	case <-ctx.Done():
	case <-d.ctx.Done():
	case ch <- result:
	}
}
