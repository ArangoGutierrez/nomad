package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	sclient "github.com/singularityware/singularity-go/cli"
	stypes "github.com/singularityware/singularity-go/pkg/types"

	multierror "github.com/hashicorp/go-multierror"
	"github.com/hashicorp/go-plugin"
	"github.com/hashicorp/nomad/client/allocdir"
	"github.com/hashicorp/nomad/client/config"
	"github.com/hashicorp/nomad/client/driver/executor"
	dstructs "github.com/hashicorp/nomad/client/driver/structs"
	cstructs "github.com/hashicorp/nomad/client/structs"
	"github.com/hashicorp/nomad/helper"
	"github.com/hashicorp/nomad/helper/fields"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/mitchellh/mapstructure"
)

const (
	// minSingularityVersion is the earliest supported version of singularity.
	minSingularityVersion = "2.3"
	// singularityInstallPrefix install prefix
	singularityInstallPrefix = "/usr/local"
	// NoSuchContainerError is returned if the container
	// does not exist.
	noSuchContainerError = "No such container"

	// The key populated in the Node Attributes to indicate the presence of the
	// Singularity driver
	singularityDriverAttr = "driver.singularity"

	// SingularityCmd is the command singularity is installed as.
	singularityCmd = "singularity"

	dir                           = "directory"
	sqshFmt, imgFmt, tarFmt, simg = "sqsh", "img", "tar", "simg"
	dckr, shub                    = "docker", "shub"
)

// SingularityDriver is a driver for running images via Singularity
type SingularityDriver struct {
	DriverContext

	Client *sclient.Client

	// A tri-state boolean to know if the fingerprinting has happened and
	// whether it has been successful
	fingerprintSuccess *bool
}

type SingularityDriverConfig struct {
	// Image config info
	ImageName   string `mapstructure:"image_name"`   // Image name, <NAME>.img
	ImagePath   string `mapstructure:"image_path"`   //	Image path, (Default /var/lib/singularity/img)
	ImageFormat string `mapstructure:"image_format"` // Image name, <NAME>.<fmt>
	//	CONTAINER FORMATS SUPPORTED
	// *.sqsh SquashFS format.  Native to Singularity 2.4+
	// *.img  This is the native Singularity image format for all
	//	  Singularity versions < 2.4.
	// *.tar*       Tar archives are exploded to a temporary directory and
	// 		run within that directory (and cleaned up after). The
	// 		contents of the archive is a root file system with root
	// 		being in the current directory. Compression suffixes as
	// 		'.gz' and '.bz2' are supported.
	// directory/          Container directories that contain a valid root file
	// 			system.
	// instance://*        A local running instance of a container. (See the
	// 			instance command group.)
	// shub://*            A container hosted on Singularity Hub
	// docker://*          A container hosted on Docker Hub

	Command string   `mapstructure:"command"`
	Args    []string `mapstructure:"args"` // Command to exec within the container
	// list of args for --insecure-options
	Env []string `mapstructure:"env"`

	App  string `mapstructure:"app"`  //	Run an app's runscript instead of the default one
	Bind string `mapstructure:"bind"` //	<spec>	A user-bind path specification.  spec has the format
	// 	src[:dest[:opts]], where src and dest are outside and
	// 	inside paths.  If dest is not given, it is set equal
	//  to src.  Mount options ('opts') may be specified as
	// 	'ro' (read-only) or 'rw' (read/write, which is the
	// 	default). This option can be called multiple times.
	Contain bool `mapstructure:"contain"` //	Use minimal /dev and empty other directories (e.g. /tmp
	//	and /home/Eduardo) instead of sharing filesystems on your host
	Containall bool   `mapstructure:"contain_all"` //	Contain not only file systems, but also PID and IPC
	Cleanenv   bool   `mapstructure:"clean_env"`   //	Clean environment before running container
	Home       string `mapstructure:"home"`        //	<spec>	A home directory specification.  spec can either be a
	//	src path or src:dest pair.  src is the source path
	//	of the home directory outside the container and dest
	//	overrides the home directory within the container
	Ipc bool `mapstructure:"ipc"` //	Run container in a new IPC namespace
	Net bool `mapstructure:"net"` //	Run container in a new network namespace (loopback is
	//	only network device active)
	Nvidia  bool   `mapstructure:"nvidia"`  //	Enable experimental Nvidia support
	Overlay string `mapstructure:"overlay"` //	Use a persistent overlayFS via a writable image
	Pid     bool   `mapstructure:"pid"`     //	Run container in a new PID namespace
	Pwd     string `mapstructure:"pwd"`     //	Initial working directory for payload process inside
	//	the container
	Scratch string `mapstructure:"scratch"` //	<path> Include a scratch directory within the container that
	//	is linked to a temporary dir (use -W to force location)
	Userns bool `mapstructure:"user_ns"` //	Run container in a new user namespace (this allows
	//	Singularity to run completely unprivileged on recent
	//	kernels and doesn't support all features)
	Workdir string `mapstructure:"work_dir"` //	Working directory to be used for /tmp, /var/tmp and
	//	/home/$USER (if -c/--contain was also used)
	Writable bool `mapstructure:"writable"` //	By default all Singularity containers are available as
	//	read only. This option makes the file system accessible
	//	as read/write.
}

// singularityHandle is returned from Start/Open as a handle to the PID
type singularityHandle struct {
	pluginClient    *plugin.Client
	executor        executor.Executor
	isolationConfig *dstructs.IsolationConfig
	userPid         int
	taskDir         *allocdir.TaskDir
	killTimeout     time.Duration
	maxKillTimeout  time.Duration
	logger          *log.Logger
	waitCh          chan *dstructs.WaitResult
	doneCh          chan struct{}
	version         string
}

// singularityPID is a struct to map the pid running the process to the vm image on
// disk
type singularityPID struct {
	Version         string
	KillTimeout     time.Duration
	MaxKillTimeout  time.Duration
	UserPid         int
	IsolationConfig *dstructs.IsolationConfig
	PluginConfig    *PluginReattachConfig
}

// Retrieve instance status for the pod with the given UUID.
func singularityGetStatus(instanceName string) (*stypes.Instance, error) {
	statusArgs := []string{
		"instance.list",
	}

	var outBuf bytes.Buffer
	cmd := exec.Command(singularityCmd, statusArgs...)
	cmd.Stdout = &outBuf
	cmd.Stderr = ioutil.Discard

	if err := cmd.Run(); err != nil {
		return nil, err
	}

	var status stypes.Instance
	if err := json.Unmarshal(outBuf.Bytes(), &status); err != nil {
		return nil, err
	}

	return &status, nil
}

// singularityRemove instance after it has exited.
func singularityRemove(instanceName string) error {
	errBuf := &bytes.Buffer{}
	cmd := exec.Command(singularityCmd, "instance.stop", instanceName)
	cmd.Stdout = ioutil.Discard
	cmd.Stderr = errBuf
	if err := cmd.Run(); err != nil {
		if msg := errBuf.String(); len(msg) > 0 {
			return fmt.Errorf("error removing pod %q: %s", instanceName, msg)
		}
		return err
	}

	return nil
}

// NewSingularityDriver is used to create a new singularity driver
func NewSingularityDriver(ctx *DriverContext) Driver {
	prefix := os.Getenv("SINGULARITY_PEFIX")
	if prefix == "" {
		prefix = singularityInstallPrefix
	}

	cli, err := sclient.NewClient(prefix)
	if err != nil {
		log.Printf("[INFO] driver.singularity: failed to initialize client: %s", err)
	}

	return &SingularityDriver{DriverContext: *ctx, Client: cli}
}

func (s *SingularityDriver) FSIsolation() cstructs.FSIsolation {
	return cstructs.FSIsolationImage
}

// Validate is used to validate the driver configuration
func (s *SingularityDriver) Validate(config map[string]interface{}) error {
	fd := &fields.FieldData{
		Raw: config,
		Schema: map[string]*fields.FieldSchema{
			"image_name": {
				Type:     fields.TypeString,
				Required: true,
			},
			"image_path": {
				Type: fields.TypeString,
			},
			"image_format": {
				Type:     fields.TypeString,
				Required: true,
			},
			"command": {
				Type:     fields.TypeString,
				Required: true,
			},
			"args": {
				Type: fields.TypeArray,
			},
			"env": {
				Type: fields.TypeArray,
			},
			"insecure_options": {
				Type: fields.TypeArray,
			},
			"app": {
				Type: fields.TypeString,
			},
			"bind": {
				Type: fields.TypeString,
			},
			"contain": {
				Type: fields.TypeBool,
			},
			"contain_all": {
				Type: fields.TypeBool,
			},
			"clean_env": {
				Type: fields.TypeBool,
			},
			"home": {
				Type: fields.TypeString,
			},
			"ipc": {
				Type: fields.TypeBool,
			},
			"net": {
				Type: fields.TypeBool,
			},
			"nvidia": {
				Type: fields.TypeBool,
			},
			"overlay": {
				Type: fields.TypeString,
			},
			"pid": {
				Type: fields.TypeBool,
			},
			"pwd": {
				Type: fields.TypeString,
			},
			"scratch": {
				Type: fields.TypeString,
			},
			"writable": {
				Type: fields.TypeBool,
			},
			"user_ns": {
				Type: fields.TypeBool,
			},
			"work_dir": {
				Type: fields.TypeString,
			},
		},
	}

	if err := fd.Validate(); err != nil {
		return err
	}

	return nil
}

func (s *SingularityDriver) Abilities() DriverAbilities {
	return DriverAbilities{
		SendSignals: false,
		Exec:        true,
	}
}

func (s *SingularityDriver) Fingerprint(cfg *config.Config, node *structs.Node) (bool, error) {
	// Client init
	cli, err := sclient.NewClient(singularityInstallPrefix)
	if err != nil {
		if s.fingerprintSuccess == nil || *s.fingerprintSuccess {
			s.logger.Printf("[INFO] driver.singularity: failed to initialize client: %s", err)
		}
		delete(node.Attributes, singularityDriverAttr)
		s.fingerprintSuccess = helper.BoolToPtr(false)
		return false, nil
	}

	node.Attributes[singularityDriverAttr] = "1"
	node.Attributes["driver.singularity.version"] = cli.ClientVersion()

	s.fingerprintSuccess = helper.BoolToPtr(true)
	return true, nil
}

func (s *SingularityDriver) Periodic() (bool, time.Duration) {
	return true, 15 * time.Second
}

func (s *SingularityDriver) Prestart(ctx *ExecContext, task *structs.Task) (*PrestartResponse, error) {
	return nil, nil
}

// Start Run an existing Singularity image.
func (s *SingularityDriver) Start(ctx *ExecContext, task *structs.Task) (*StartResponse, error) {
	var driverConfig SingularityDriverConfig
	var err error

	if err := mapstructure.WeakDecode(task.Config, &driverConfig); err != nil {
		return nil, err
	}

	// Global arguments given to both prepare and run-prepared
	runArgs := make([]string, 0, 50)
	execArgs := make([]string, 0, 50)

	image := stypes.ImageFmt{
		Name:   driverConfig.ImageName,
		Format: driverConfig.ImageFormat,
		Path:   driverConfig.ImagePath,
	}

	switch driverConfig.Command {
	case "exec":
		execOptions := stypes.ContainerRunOptions{
			App:          driverConfig.App,
			Bind:         driverConfig.Bind,
			Contain:      driverConfig.Contain,
			Containall:   driverConfig.Containall,
			Cleanenv:     driverConfig.Cleanenv,
			Home:         driverConfig.Home,
			Ipc:          driverConfig.Ipc,
			Net:          driverConfig.Net,
			Nvidia:       driverConfig.Nvidia,
			Overlay:      driverConfig.Overlay,
			Pid:          driverConfig.Pid,
			Pwd:          driverConfig.Pwd,
			Scratch:      driverConfig.Scratch,
			Userns:       driverConfig.Userns,
			Workdir:      driverConfig.Workdir,
			Writable:     driverConfig.Writable,
			ContainerFmt: image,
			Args:         driverConfig.Args,
		}
		execArgs, err = sclient.ContainerRunOptions(s.Client, execOptions, "exec")
		if err != nil {
			_ = fmt.Errorf("%v", err)
			s.logger.Printf("[DEBUG] driver.singularity: Couldn't retrieve command & args for exec (not specified): %v", err)
			return nil, err
		}
	case "run":
		execOptions := stypes.ContainerRunOptions{
			App:          driverConfig.App,
			Bind:         driverConfig.Bind,
			Contain:      driverConfig.Contain,
			Containall:   driverConfig.Containall,
			Cleanenv:     driverConfig.Cleanenv,
			Home:         driverConfig.Home,
			Ipc:          driverConfig.Ipc,
			Net:          driverConfig.Net,
			Nvidia:       driverConfig.Nvidia,
			Overlay:      driverConfig.Overlay,
			Pid:          driverConfig.Pid,
			Pwd:          driverConfig.Pwd,
			Scratch:      driverConfig.Scratch,
			Userns:       driverConfig.Userns,
			Workdir:      driverConfig.Workdir,
			Writable:     driverConfig.Writable,
			ContainerFmt: image,
			Args:         driverConfig.Args,
		}

		execArgs, err = sclient.ContainerRunOptions(s.Client, execOptions, "run")
		if err != nil {
			_ = fmt.Errorf("%v", err)
			s.logger.Printf("[DEBUG] driver.singularity: Couldn't retrieve command & args for exec (not specified): %v", err)
			return nil, err
		}
	case "instance.start":
		istartOptions := &stypes.InstanceStartOptions{
			Bind:         driverConfig.Bind,
			Contain:      driverConfig.Contain,
			Home:         driverConfig.Home,
			Net:          driverConfig.Net,
			Nvidia:       driverConfig.Nvidia,
			Overlay:      driverConfig.Overlay,
			Scratch:      driverConfig.Scratch,
			Workdir:      driverConfig.Workdir,
			Writable:     driverConfig.Writable,
			ContainerFmt: image,
		}
		execArgs, err = sclient.InstanceStartOptions(s.Client, istartOptions)
		if err != nil {
			_ = fmt.Errorf("%v", err)
			s.logger.Printf("[DEBUG] driver.singularity: Couldn't retrieve  command & args for instance.start (not specified): %v", err)
			return nil, err
		}
	case "":
		err = errors.New("Command not specified")
		_ = fmt.Errorf("%v", err)
		s.logger.Printf("[DEBUG] driver.singularity: Couldn't retrieve command (not specified): %v", err)
		return nil, err
	}

	runArgs = append(runArgs, execArgs...)

	// Get the command to be ran
	command := driverConfig.Command
	if err := validateCommand(command, "args"); err != nil {
		return nil, err
	}

	pluginLogFile := filepath.Join(ctx.TaskDir.Dir, "executor.out")
	executorConfig := &dstructs.ExecutorConfig{
		LogFile:  pluginLogFile,
		LogLevel: s.config.LogLevel,
	}

	exec, pluginClient, err := createExecutor(s.config.LogOutput, s.config, executorConfig)
	if err != nil {
		return nil, err
	}

	// Singularity image
	img := driverConfig.ImageName

	executorCtx := &executor.ExecutorContext{
		TaskEnv: ctx.TaskEnv,
		Driver:  "singularity",
		AllocID: s.DriverContext.allocID,
		LogDir:  ctx.TaskDir.LogDir,
		TaskDir: ctx.TaskDir.Dir,
		Task:    task,
	}
	if err := exec.SetContext(executorCtx); err != nil {
		pluginClient.Kill()
		return nil, fmt.Errorf("failed to set executor context: %v", err)
	}

	execCmd := &executor.ExecCommand{
		Cmd:            singularityCmd,
		Args:           runArgs,
		FSIsolation:    true,
		ResourceLimits: true,
		User:           getExecutorUser(task),
	}

	ps, err := exec.LaunchCmd(execCmd)
	if err != nil {
		pluginClient.Kill()
		return nil, err
	}

	s.logger.Printf("[DEBUG] driver.singularity: \nstarted container %q for task %q with: %v", img, s.taskName, runArgs)
	// Return a driver handle
	maxKill := s.DriverContext.config.MaxKillTimeout
	h := &singularityHandle{
		pluginClient:    pluginClient,
		userPid:         ps.Pid,
		executor:        exec,
		isolationConfig: ps.IsolationConfig,
		killTimeout:     GetKillTimeout(task.KillTimeout, maxKill),
		maxKillTimeout:  maxKill,
		logger:          s.logger,
		version:         s.config.Version.VersionNumber(),
		doneCh:          make(chan struct{}),
		waitCh:          make(chan *dstructs.WaitResult, 1),
		taskDir:         ctx.TaskDir,
	}

	go h.run()
	return &StartResponse{Handle: h}, nil
}

func (s *SingularityDriver) Cleanup(*ExecContext, *CreatedResources) error { return nil }

func (s *SingularityDriver) Open(ctx *ExecContext, handleID string) (DriverHandle, error) {
	id := &execId{}
	if err := json.Unmarshal([]byte(handleID), id); err != nil {
		return nil, fmt.Errorf("Failed to parse handle '%s': %v", handleID, err)
	}

	pluginConfig := &plugin.ClientConfig{
		Reattach: id.PluginConfig.PluginConfig(),
	}
	exec, client, err := createExecutorWithConfig(pluginConfig, s.config.LogOutput)
	if err != nil {
		merrs := new(multierror.Error)
		merrs.Errors = append(merrs.Errors, err)
		s.logger.Println("[ERR] driver.exec: error connecting to plugin so destroying plugin pid and user pid")
		if e := destroyPlugin(id.PluginConfig.Pid, id.UserPid); e != nil {
			merrs.Errors = append(merrs.Errors, fmt.Errorf("error destroying plugin and userpid: %v", e))
		}
		if id.IsolationConfig != nil {
			ePid := pluginConfig.Reattach.Pid
			if e := executor.ClientCleanup(id.IsolationConfig, ePid); e != nil {
				merrs.Errors = append(merrs.Errors, fmt.Errorf("destroying cgroup failed: %v", e))
			}
		}
		return nil, fmt.Errorf("error connecting to plugin: %v", merrs.ErrorOrNil())
	}

	ver, _ := exec.Version()
	s.logger.Printf("[DEBUG] driver.exec : version of executor: %v", ver.Version)
	// Return a driver handle
	h := &singularityHandle{
		pluginClient:    client,
		executor:        exec,
		userPid:         id.UserPid,
		isolationConfig: id.IsolationConfig,
		logger:          s.logger,
		version:         id.Version,
		killTimeout:     id.KillTimeout,
		maxKillTimeout:  id.MaxKillTimeout,
		doneCh:          make(chan struct{}),
		waitCh:          make(chan *dstructs.WaitResult, 1),
		taskDir:         ctx.TaskDir,
	}
	go h.run()
	return h, nil
}

func (sh *singularityHandle) ID() string {
	// Return a handle to the PID
	pid := &singularityPID{
		Version:         sh.version,
		KillTimeout:     sh.killTimeout,
		MaxKillTimeout:  sh.maxKillTimeout,
		PluginConfig:    NewPluginReattachConfig(sh.pluginClient.ReattachConfig()),
		UserPid:         sh.userPid,
		IsolationConfig: sh.isolationConfig,
	}

	data, err := json.Marshal(pid)
	if err != nil {
		sh.logger.Printf("[ERR] driver.singularity: failed to marshal singularity PID to JSON: %s", err)
	}
	return fmt.Sprintf("Singularity:%s", string(data))
}

func (sh *singularityHandle) WaitCh() chan *dstructs.WaitResult {
	return sh.waitCh
}

func (sh *singularityHandle) Update(task *structs.Task) error {
	// Store the updated kill timeout.
	sh.killTimeout = GetKillTimeout(task.KillTimeout, sh.maxKillTimeout)
	sh.executor.UpdateTask(task)

	// Update is not possible
	return nil
}

func (sh *singularityHandle) Exec(ctx context.Context, cmd string, args []string) ([]byte, int, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		// No deadline set on context; default to 1 minute
		deadline = time.Now().Add(time.Minute)
	}
	return sh.executor.Exec(deadline, cmd, args)
}

func (sh *singularityHandle) Signal(s os.Signal) error {
	return fmt.Errorf("Singularity does not support signals")
}

// Kill is used to terminate the task. We send an Interrupt
// and then provide a 5 second grace period before doing a Kill.
func (sh *singularityHandle) Kill() error {
	sh.executor.ShutDown()
	select {
	case <-sh.doneCh:
		return nil
	case <-time.After(sh.killTimeout):
		return sh.executor.Exit()
	}
}

func (sh *singularityHandle) Stats() (*cstructs.TaskResourceUsage, error) {
	return nil, DriverStatsNotImplemented
}

func (sh *singularityHandle) run() {
	ps, werr := sh.executor.Wait()
	close(sh.doneCh)
	if ps.ExitCode == 0 && werr != nil {
		if e := killProcess(sh.userPid); e != nil {
			sh.logger.Printf("[ERR] driver.singularity: error killing user process: %v", e)
		}
	}

	// Exit the executor
	if err := sh.executor.Exit(); err != nil {
		sh.logger.Printf("[ERR] driver.singularity: error killing executor: %v", err)
	}
	sh.pluginClient.Kill()

	// Send the results
	sh.waitCh <- &dstructs.WaitResult{ExitCode: ps.ExitCode, Signal: ps.Signal, Err: werr}
	close(sh.waitCh)
}
