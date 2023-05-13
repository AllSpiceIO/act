package runner

import (
	"archive/tar"
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/opencontainers/selinux/go-selinux"

	"github.com/nektos/act/pkg/common"
	"github.com/nektos/act/pkg/container"
	"github.com/nektos/act/pkg/exprparser"
	"github.com/nektos/act/pkg/model"
)

// RunContext contains info about current job
type RunContext struct {
	Name                string
	Config              *Config
	Matrix              map[string]interface{}
	Run                 *model.Run
	EventJSON           string
	Env                 map[string]string
	GlobalEnv           map[string]string // to pass env changes of GITHUB_ENV and set-env correctly, due to dirty Env field
	ExtraPath           []string
	CurrentStep         string
	StepResults         map[string]*model.StepResult
	IntraActionState    map[string]map[string]string
	ExprEval            ExpressionEvaluator
	JobContainer        container.ExecutionsEnvironment
	ServiceContainers   []container.ExecutionsEnvironment
	OutputMappings      map[MappableOutput]MappableOutput
	JobName             string
	ActionPath          string
	Parent              *RunContext
	Masks               []string
	cleanUpJobContainer common.Executor
	caller              *caller // job calling this RunContext (reusable workflows)
}

func (rc *RunContext) AddMask(mask string) {
	rc.Masks = append(rc.Masks, mask)
}

type MappableOutput struct {
	StepID     string
	OutputName string
}

func (rc *RunContext) String() string {
	name := fmt.Sprintf("%s/%s", rc.Run.Workflow.Name, rc.Name)
	if rc.caller != nil {
		// prefix the reusable workflow with the caller job
		// this is required to create unique container names
		name = fmt.Sprintf("%s/%s", rc.caller.runContext.Run.JobID, name)
	}
	return name
}

// GetEnv returns the env for the context
func (rc *RunContext) GetEnv() map[string]string {
	if rc.Env == nil {
		rc.Env = map[string]string{}
		if rc.Run != nil && rc.Run.Workflow != nil && rc.Config != nil {
			job := rc.Run.Job()
			if job != nil {
				rc.Env = mergeMaps(rc.Run.Workflow.Env, job.Environment(), rc.Config.Env)
			}
		}
	}
	rc.Env["ACT"] = "true"
	return rc.Env
}

func (rc *RunContext) jobContainerName() string {
	return createSimpleContainerName(rc.Config.ContainerNamePrefix, "WORKFLOW-"+rc.Run.Workflow.Name, "JOB-"+rc.Name)
}

func getDockerDaemonSocketMountPath(daemonPath string) string {
	if protoIndex := strings.Index(daemonPath, "://"); protoIndex != -1 {
		scheme := daemonPath[:protoIndex]
		if strings.EqualFold(scheme, "npipe") {
			// linux container mount on windows, use the default socket path of the VM / wsl2
			return "/var/run/docker.sock"
		} else if strings.EqualFold(scheme, "unix") {
			return daemonPath[protoIndex+3:]
		} else if strings.IndexFunc(scheme, func(r rune) bool {
			return (r < 'a' || r > 'z') && (r < 'A' || r > 'Z')
		}) == -1 {
			// unknown protocol use default
			return "/var/run/docker.sock"
		}
	}
	return daemonPath
}

// Returns the binds and mounts for the container, resolving paths as appopriate
func (rc *RunContext) GetBindsAndMounts() ([]string, map[string]string) {
	name := rc.jobContainerName()

	if rc.Config.ContainerDaemonSocket == "" {
		rc.Config.ContainerDaemonSocket = "/var/run/docker.sock"
	}

	binds := []string{}
	if rc.Config.ContainerDaemonSocket != "-" {
		daemonPath := getDockerDaemonSocketMountPath(rc.Config.ContainerDaemonSocket)
		binds = append(binds, fmt.Sprintf("%s:%s", daemonPath, "/var/run/docker.sock"))
	}

	ext := container.LinuxContainerEnvironmentExtensions{}

	mounts := map[string]string{
		"act-toolcache": "/toolcache",
		name + "-env":   ext.GetActPath(),
	}

	if job := rc.Run.Job(); job != nil {
		if container := job.Container(); container != nil {
			for _, v := range container.Volumes {
				if !strings.Contains(v, ":") || filepath.IsAbs(v) {
					// Bind anonymous volume or host file.
					binds = append(binds, v)
				} else {
					// Mount existing volume.
					paths := strings.SplitN(v, ":", 2)
					mounts[paths[0]] = paths[1]
				}
			}
		}
	}

	if rc.Config.BindWorkdir {
		bindModifiers := ""
		if runtime.GOOS == "darwin" {
			bindModifiers = ":delegated"
		}
		if selinux.GetEnabled() {
			bindModifiers = ":z"
		}
		binds = append(binds, fmt.Sprintf("%s:%s%s", rc.Config.Workdir, ext.ToContainerPath(rc.Config.Workdir), bindModifiers))
	} else {
		mounts[name] = ext.ToContainerPath(rc.Config.Workdir)
	}

	return binds, mounts
}

func (rc *RunContext) startHostEnvironment() common.Executor {
	return func(ctx context.Context) error {
		logger := common.Logger(ctx)
		rawLogger := logger.WithField("raw_output", true)
		logWriter := common.NewLineWriter(rc.commandHandler(ctx), func(s string) bool {
			if rc.Config.LogOutput {
				rawLogger.Infof("%s", s)
			} else {
				rawLogger.Debugf("%s", s)
			}
			return true
		})
		cacheDir := rc.ActionCacheDir()
		randBytes := make([]byte, 8)
		_, _ = rand.Read(randBytes)
		miscpath := filepath.Join(cacheDir, hex.EncodeToString(randBytes))
		actPath := filepath.Join(miscpath, "act")
		if err := os.MkdirAll(actPath, 0o777); err != nil {
			return err
		}
		path := filepath.Join(miscpath, "hostexecutor")
		if err := os.MkdirAll(path, 0o777); err != nil {
			return err
		}
		runnerTmp := filepath.Join(miscpath, "tmp")
		if err := os.MkdirAll(runnerTmp, 0o777); err != nil {
			return err
		}
		toolCache := filepath.Join(cacheDir, "tool_cache")
		rc.JobContainer = &container.HostEnvironment{
			Path:      path,
			TmpDir:    runnerTmp,
			ToolCache: toolCache,
			Workdir:   rc.Config.Workdir,
			ActPath:   actPath,
			CleanUp: func() {
				os.RemoveAll(miscpath)
			},
			StdOut: logWriter,
		}
		rc.cleanUpJobContainer = rc.JobContainer.Remove()
		for k, v := range rc.JobContainer.GetRunnerContext(ctx) {
			if v, ok := v.(string); ok {
				rc.Env[fmt.Sprintf("RUNNER_%s", strings.ToUpper(k))] = v
			}
		}
		for _, env := range os.Environ() {
			if k, v, ok := strings.Cut(env, "="); ok {
				// don't override
				if _, ok := rc.Env[k]; !ok {
					rc.Env[k] = v
				}
			}
		}

		return common.NewPipelineExecutor(
			rc.JobContainer.Copy(rc.JobContainer.GetActPath()+"/", &container.FileEntry{
				Name: "workflow/event.json",
				Mode: 0o644,
				Body: rc.EventJSON,
			}, &container.FileEntry{
				Name: "workflow/envs.txt",
				Mode: 0o666,
				Body: "",
			}),
		)(ctx)
	}
}

func (rc *RunContext) startJobContainer() common.Executor {
	return func(ctx context.Context) error {
		logger := common.Logger(ctx)
		image := rc.platformImage(ctx)
		rawLogger := logger.WithField("raw_output", true)
		logWriter := common.NewLineWriter(rc.commandHandler(ctx), func(s string) bool {
			if rc.Config.LogOutput {
				rawLogger.Infof("%s", s)
			} else {
				rawLogger.Debugf("%s", s)
			}
			return true
		})

		username, password, err := rc.handleCredentials(ctx)
		if err != nil {
			return fmt.Errorf("failed to handle credentials: %s", err)
		}

		logger.Infof("\U0001f680  Start image=%s", image)
		name := rc.jobContainerName()

		envList := make([]string, 0)

		envList = append(envList, fmt.Sprintf("%s=%s", "RUNNER_TOOL_CACHE", "/opt/hostedtoolcache"))
		envList = append(envList, fmt.Sprintf("%s=%s", "RUNNER_OS", "Linux"))
		envList = append(envList, fmt.Sprintf("%s=%s", "RUNNER_ARCH", container.RunnerArch(ctx)))
		envList = append(envList, fmt.Sprintf("%s=%s", "RUNNER_TEMP", "/tmp"))
		envList = append(envList, fmt.Sprintf("%s=%s", "LANG", "C.UTF-8")) // Use same locale as GitHub Actions

		ext := container.LinuxContainerEnvironmentExtensions{}
		binds, mounts := rc.GetBindsAndMounts()

		networkName := string(rc.Config.ContainerNetworkMode)
		if rc.Config.NeedCreateNetwork {
			networkName = fmt.Sprintf("%s-network", rc.jobContainerName())
		}

		// add service containers
		for serviceId, spec := range rc.Run.Job().Services {
			// interpolate env
			interpolatedEnvs := make(map[string]string, len(spec.Env))
			for k, v := range spec.Env {
				interpolatedEnvs[k] = rc.ExprEval.Interpolate(ctx, v)
			}
			envs := make([]string, 0, len(interpolatedEnvs))
			for k, v := range interpolatedEnvs {
				envs = append(envs, fmt.Sprintf("%s=%s", k, v))
			}
			// interpolate cmd
			interpolatedCmd := make([]string, 0, len(spec.Cmd))
			for _, v := range spec.Cmd {
				interpolatedCmd = append(interpolatedCmd, rc.ExprEval.Interpolate(ctx, v))
			}
			username, password, err := rc.handleServiceCredentials(ctx, spec.Credentials)
			if err != nil {
				return fmt.Errorf("failed to handle service %s credentials: %w", serviceId, err)
			}
			serviceContainerName := createSimpleContainerName(rc.jobContainerName(), serviceId)
			c := container.NewContainer(&container.NewContainerInput{
				Name:       serviceContainerName,
				WorkingDir: ext.ToContainerPath(rc.Config.Workdir),
				Image:      spec.Image,
				Username:   username,
				Password:   password,
				Cmd:        interpolatedCmd,
				Env:        envs,
				Mounts: map[string]string{
					// TODO merge volumes
					serviceId:       ext.ToContainerPath(rc.Config.Workdir),
					"act-toolcache": "/toolcache",
					"act-actions":   "/actions",
				},
				Binds:          binds,
				Stdout:         logWriter,
				Stderr:         logWriter,
				Privileged:     rc.Config.Privileged,
				UsernsMode:     rc.Config.UsernsMode,
				Platform:       rc.Config.ContainerArchitecture,
				AutoRemove:     rc.Config.AutoRemove,
				Options:        spec.Options,
				NetworkMode:    networkName,
				NetworkAliases: []string{serviceId},
			})
			rc.ServiceContainers = append(rc.ServiceContainers, c)
		}

		rc.cleanUpJobContainer = func(ctx context.Context) error {
			if rc.JobContainer != nil && !rc.Config.ReuseContainers {
				return rc.JobContainer.Remove().
					Then(container.NewDockerVolumeRemoveExecutor(rc.jobContainerName(), false)).
					Then(container.NewDockerVolumeRemoveExecutor(rc.jobContainerName()+"-env", false))(ctx)
			}
			return nil
		}

		rc.JobContainer = container.NewContainer(&container.NewContainerInput{
			Cmd:            nil,
			Entrypoint:     []string{"/bin/sleep", fmt.Sprint(rc.Config.ContainerMaxLifetime.Round(time.Second).Seconds())},
			WorkingDir:     ext.ToContainerPath(rc.Config.Workdir),
			Image:          image,
			Username:       username,
			Password:       password,
			Name:           name,
			Env:            envList,
			Mounts:         mounts,
			NetworkMode:    networkName,
			NetworkAliases: []string{rc.Name},
			Binds:          binds,
			Stdout:         logWriter,
			Stderr:         logWriter,
			Privileged:     rc.Config.Privileged,
			UsernsMode:     rc.Config.UsernsMode,
			Platform:       rc.Config.ContainerArchitecture,
			Options:        rc.options(ctx),
			AutoRemove:     rc.Config.AutoRemove,
		})
		if rc.JobContainer == nil {
			return errors.New("Failed to create job container")
		}

		return common.NewPipelineExecutor(
			rc.pullServicesImages(rc.Config.ForcePull),
			rc.JobContainer.Pull(rc.Config.ForcePull),
			rc.createNetwork(networkName).IfBool(rc.Config.NeedCreateNetwork),
			rc.startServiceContainers(networkName),
			rc.JobContainer.Create(rc.Config.ContainerCapAdd, rc.Config.ContainerCapDrop),
			rc.JobContainer.Start(false),
			rc.JobContainer.Copy(rc.JobContainer.GetActPath()+"/", &container.FileEntry{
				Name: "workflow/event.json",
				Mode: 0o644,
				Body: rc.EventJSON,
			}, &container.FileEntry{
				Name: "workflow/envs.txt",
				Mode: 0o666,
				Body: "",
			}),
		)(ctx)
	}
}

func (rc *RunContext) createNetwork(name string) common.Executor {
	return func(ctx context.Context) error {
		return container.NewDockerNetworkCreateExecutor(name)(ctx)
	}
}

func (rc *RunContext) removeNetwork(name string) common.Executor {
	return func(ctx context.Context) error {
		return container.NewDockerNetworkRemoveExecutor(name)(ctx)
	}
}

func (rc *RunContext) execJobContainer(cmd []string, env map[string]string, user, workdir string) common.Executor {
	return func(ctx context.Context) error {
		return rc.JobContainer.Exec(cmd, env, user, workdir)(ctx)
	}
}

func (rc *RunContext) ApplyExtraPath(ctx context.Context, env *map[string]string) {
	if rc.ExtraPath != nil && len(rc.ExtraPath) > 0 {
		path := rc.JobContainer.GetPathVariableName()
		if rc.JobContainer.IsEnvironmentCaseInsensitive() {
			// On windows system Path and PATH could also be in the map
			for k := range *env {
				if strings.EqualFold(path, k) {
					path = k
					break
				}
			}
		}
		if (*env)[path] == "" {
			cenv := map[string]string{}
			var cpath string
			if err := rc.JobContainer.UpdateFromImageEnv(&cenv)(ctx); err == nil {
				if p, ok := cenv[path]; ok {
					cpath = p
				}
			}
			if len(cpath) == 0 {
				cpath = rc.JobContainer.DefaultPathVariable()
			}
			(*env)[path] = cpath
		}
		(*env)[path] = rc.JobContainer.JoinPathVariable(append(rc.ExtraPath, (*env)[path])...)
	}
}

func (rc *RunContext) UpdateExtraPath(ctx context.Context, githubEnvPath string) error {
	if common.Dryrun(ctx) {
		return nil
	}
	pathTar, err := rc.JobContainer.GetContainerArchive(ctx, githubEnvPath)
	if err != nil {
		return err
	}
	defer pathTar.Close()

	reader := tar.NewReader(pathTar)
	_, err = reader.Next()
	if err != nil && err != io.EOF {
		return err
	}
	s := bufio.NewScanner(reader)
	for s.Scan() {
		line := s.Text()
		if len(line) > 0 {
			rc.addPath(ctx, line)
		}
	}
	return nil
}

// stopJobContainer removes the job container (if it exists) and its volume (if it exists) if !rc.Config.ReuseContainers
func (rc *RunContext) stopJobContainer() common.Executor {
	return func(ctx context.Context) error {
		if rc.cleanUpJobContainer != nil && !rc.Config.ReuseContainers {
			return rc.cleanUpJobContainer(ctx)
		}
		return nil
	}
}

func (rc *RunContext) pullServicesImages(forcePull bool) common.Executor {
	return func(ctx context.Context) error {
		execs := []common.Executor{}
		for _, c := range rc.ServiceContainers {
			execs = append(execs, c.Pull(forcePull))
		}
		return common.NewParallelExecutor(len(execs), execs...)(ctx)
	}
}

func (rc *RunContext) startServiceContainers(networkName string) common.Executor {
	return func(ctx context.Context) error {
		execs := []common.Executor{}
		for _, c := range rc.ServiceContainers {
			execs = append(execs, common.NewPipelineExecutor(
				c.Pull(false),
				c.Create(rc.Config.ContainerCapAdd, rc.Config.ContainerCapDrop),
				c.Start(false),
			))
		}
		return common.NewParallelExecutor(len(execs), execs...)(ctx)
	}
}

func (rc *RunContext) stopServiceContainers(networkName string) common.Executor {
	return func(ctx context.Context) error {
		execs := []common.Executor{}
		for _, c := range rc.ServiceContainers {
			execs = append(execs, common.NewPipelineExecutor(
				c.Remove(),
			))
		}
		return common.NewParallelExecutor(len(execs), execs...)(ctx)
	}
}

// Prepare the mounts and binds for the worker

// ActionCacheDir is for rc
func (rc *RunContext) ActionCacheDir() string {
	var xdgCache string
	var ok bool
	if xdgCache, ok = os.LookupEnv("XDG_CACHE_HOME"); !ok || xdgCache == "" {
		if home, err := os.UserHomeDir(); err == nil {
			xdgCache = filepath.Join(home, ".cache")
		} else if xdgCache, err = filepath.Abs("."); err != nil {
			// It's almost impossible to get here, so the temp dir is a good fallback
			xdgCache = os.TempDir()
		}
	}
	return filepath.Join(xdgCache, "act")
}

// Interpolate outputs after a job is done
func (rc *RunContext) interpolateOutputs() common.Executor {
	return func(ctx context.Context) error {
		ee := rc.NewExpressionEvaluator(ctx)
		for k, v := range rc.Run.Job().Outputs {
			interpolated := ee.Interpolate(ctx, v)
			if v != interpolated {
				rc.Run.Job().Outputs[k] = interpolated
			}
		}
		return nil
	}
}

func (rc *RunContext) startContainer() common.Executor {
	return func(ctx context.Context) error {
		if rc.IsHostEnv(ctx) {
			return rc.startHostEnvironment()(ctx)
		}
		return rc.startJobContainer()(ctx)
	}
}

func (rc *RunContext) IsHostEnv(ctx context.Context) bool {
	image := rc.platformImage(ctx)
	return strings.EqualFold(image, "-self-hosted")
}

func (rc *RunContext) stopContainer() common.Executor {
	return rc.stopJobContainer()
}

func (rc *RunContext) closeContainer() common.Executor {
	return func(ctx context.Context) error {
		if rc.JobContainer != nil {
			return rc.JobContainer.Close()(ctx)
		}
		return nil
	}
}

func (rc *RunContext) matrix() map[string]interface{} {
	return rc.Matrix
}

func (rc *RunContext) result(result string) {
	rc.Run.Job().Result = result
}

func (rc *RunContext) steps() []*model.Step {
	return rc.Run.Job().Steps
}

// Executor returns a pipeline executor for all the steps in the job
func (rc *RunContext) Executor() common.Executor {
	var executor common.Executor

	switch rc.Run.Job().Type() {
	case model.JobTypeDefault:
		executor = newJobExecutor(rc, &stepFactoryImpl{}, rc)
	case model.JobTypeReusableWorkflowLocal:
		executor = newLocalReusableWorkflowExecutor(rc)
	case model.JobTypeReusableWorkflowRemote:
		executor = newRemoteReusableWorkflowExecutor(rc)
	}

	return func(ctx context.Context) error {
		res, err := rc.isEnabled(ctx)
		if err != nil {
			return err
		}
		if res {
			return executor(ctx)
		}
		return nil
	}
}

func (rc *RunContext) platformImage(ctx context.Context) string {
	job := rc.Run.Job()

	c := job.Container()
	if c != nil {
		return rc.ExprEval.Interpolate(ctx, c.Image)
	}

	if job.RunsOn() == nil {
		common.Logger(ctx).Errorf("'runs-on' key not defined in %s", rc.String())
	}

	runsOn := job.RunsOn()
	for i, v := range runsOn {
		runsOn[i] = rc.ExprEval.Interpolate(ctx, v)
	}

	if pick := rc.Config.PlatformPicker; pick != nil {
		if image := pick(runsOn); image != "" {
			return image
		}
	}

	for _, runnerLabel := range runsOn {
		image := rc.Config.Platforms[strings.ToLower(runnerLabel)]
		if image != "" {
			return image
		}
	}

	return ""
}

func (rc *RunContext) options(ctx context.Context) string {
	job := rc.Run.Job()
	c := job.Container()
	if c == nil {
		return rc.Config.ContainerOptions
	}

	return c.Options
}

func (rc *RunContext) isEnabled(ctx context.Context) (bool, error) {
	job := rc.Run.Job()
	l := common.Logger(ctx)
	runJob, err := EvalBool(ctx, rc.ExprEval, job.If.Value, exprparser.DefaultStatusCheckSuccess)
	if err != nil {
		return false, fmt.Errorf("  \u274C  Error in if-expression: \"if: %s\" (%s)", job.If.Value, err)
	}
	if !runJob {
		l.WithField("jobResult", "skipped").Debugf("Skipping job '%s' due to '%s'", job.Name, job.If.Value)
		return false, nil
	}

	if job.Type() != model.JobTypeDefault {
		return true, nil
	}

	img := rc.platformImage(ctx)
	if img == "" {
		if job.RunsOn() == nil {
			l.Errorf("'runs-on' key not defined in %s", rc.String())
		}

		for _, runnerLabel := range job.RunsOn() {
			platformName := rc.ExprEval.Interpolate(ctx, runnerLabel)
			l.Infof("\U0001F6A7  Skipping unsupported platform -- Try running with `-P %+v=...`", platformName)
		}
		return false, nil
	}
	return true, nil
}

func mergeMaps(maps ...map[string]string) map[string]string {
	rtnMap := make(map[string]string)
	for _, m := range maps {
		for k, v := range m {
			rtnMap[k] = v
		}
	}
	return rtnMap
}

// deprecated: use createSimpleContainerName
func createContainerName(parts ...string) string {
	name := strings.Join(parts, "-")
	pattern := regexp.MustCompile("[^a-zA-Z0-9]")
	name = pattern.ReplaceAllString(name, "-")
	name = strings.ReplaceAll(name, "--", "-")
	hash := sha256.Sum256([]byte(name))

	// SHA256 is 64 hex characters. So trim name to 63 characters to make room for the hash and separator
	trimmedName := strings.Trim(trimToLen(name, 63), "-")

	return fmt.Sprintf("%s-%x", trimmedName, hash)
}

func createSimpleContainerName(parts ...string) string {
	pattern := regexp.MustCompile("[^a-zA-Z0-9-]")
	name := make([]string, 0, len(parts))
	for _, v := range parts {
		v = pattern.ReplaceAllString(v, "-")
		v = strings.Trim(v, "-")
		for strings.Contains(v, "--") {
			v = strings.ReplaceAll(v, "--", "-")
		}
		if v != "" {
			name = append(name, v)
		}
	}
	return strings.Join(name, "_")
}

func trimToLen(s string, l int) string {
	if l < 0 {
		l = 0
	}
	if len(s) > l {
		return s[:l]
	}
	return s
}

func (rc *RunContext) getJobContext() *model.JobContext {
	jobStatus := "success"
	for _, stepStatus := range rc.StepResults {
		if stepStatus.Conclusion == model.StepStatusFailure {
			jobStatus = "failure"
			break
		}
	}
	return &model.JobContext{
		Status: jobStatus,
	}
}

func (rc *RunContext) getStepsContext() map[string]*model.StepResult {
	return rc.StepResults
}

func (rc *RunContext) getVarsContext() map[string]string {
	return rc.Config.Vars
}

func (rc *RunContext) getGithubContext(ctx context.Context) *model.GithubContext {
	logger := common.Logger(ctx)
	ghc := &model.GithubContext{
		Event:            make(map[string]interface{}),
		Workflow:         rc.Run.Workflow.Name,
		RunID:            rc.Config.Env["GITHUB_RUN_ID"],
		RunNumber:        rc.Config.Env["GITHUB_RUN_NUMBER"],
		Actor:            rc.Config.Actor,
		EventName:        rc.Config.EventName,
		Action:           rc.CurrentStep,
		Token:            rc.Config.Token,
		Job:              rc.Run.JobID,
		ActionPath:       rc.ActionPath,
		RepositoryOwner:  rc.Config.Env["GITHUB_REPOSITORY_OWNER"],
		RetentionDays:    rc.Config.Env["GITHUB_RETENTION_DAYS"],
		RunnerPerflog:    rc.Config.Env["RUNNER_PERFLOG"],
		RunnerTrackingID: rc.Config.Env["RUNNER_TRACKING_ID"],
		Repository:       rc.Config.Env["GITHUB_REPOSITORY"],
		Ref:              rc.Config.Env["GITHUB_REF"],
		Sha:              rc.Config.Env["SHA_REF"],
		RefName:          rc.Config.Env["GITHUB_REF_NAME"],
		RefType:          rc.Config.Env["GITHUB_REF_TYPE"],
		BaseRef:          rc.Config.Env["GITHUB_BASE_REF"],
		HeadRef:          rc.Config.Env["GITHUB_HEAD_REF"],
		Workspace:        rc.Config.Env["GITHUB_WORKSPACE"],
	}
	if rc.JobContainer != nil {
		ghc.EventPath = rc.JobContainer.GetActPath() + "/workflow/event.json"
		ghc.Workspace = rc.JobContainer.ToContainerPath(rc.Config.Workdir)
	}

	if ghc.RunID == "" {
		ghc.RunID = "1"
	}

	if ghc.RunNumber == "" {
		ghc.RunNumber = "1"
	}

	if ghc.RetentionDays == "" {
		ghc.RetentionDays = "0"
	}

	if ghc.RunnerPerflog == "" {
		ghc.RunnerPerflog = "/dev/null"
	}

	// Backwards compatibility for configs that require
	// a default rather than being run as a cmd
	if ghc.Actor == "" {
		ghc.Actor = "nektos/act"
	}

	{ // Adapt to Gitea
		if preset := rc.Config.PresetGitHubContext; preset != nil {
			ghc.Event = preset.Event
			ghc.RunID = preset.RunID
			ghc.RunNumber = preset.RunNumber
			ghc.Actor = preset.Actor
			ghc.Repository = preset.Repository
			ghc.EventName = preset.EventName
			ghc.Sha = preset.Sha
			ghc.Ref = preset.Ref
			ghc.RefName = preset.RefName
			ghc.RefType = preset.RefType
			ghc.HeadRef = preset.HeadRef
			ghc.BaseRef = preset.BaseRef
			ghc.Token = preset.Token
			ghc.RepositoryOwner = preset.RepositoryOwner
			ghc.RetentionDays = preset.RetentionDays
			return ghc
		}
	}

	if rc.EventJSON != "" {
		err := json.Unmarshal([]byte(rc.EventJSON), &ghc.Event)
		if err != nil {
			logger.Errorf("Unable to Unmarshal event '%s': %v", rc.EventJSON, err)
		}
	}

	ghc.SetBaseAndHeadRef()
	repoPath := rc.Config.Workdir
	ghc.SetRepositoryAndOwner(ctx, rc.Config.GitHubInstance, rc.Config.RemoteName, repoPath)
	if ghc.Ref == "" {
		ghc.SetRef(ctx, rc.Config.DefaultBranch, repoPath)
	}
	if ghc.Sha == "" {
		ghc.SetSha(ctx, repoPath)
	}

	ghc.SetRefTypeAndName()

	// defaults
	ghc.ServerURL = "https://github.com"
	ghc.APIURL = "https://api.github.com"
	ghc.GraphQLURL = "https://api.github.com/graphql"
	// per GHES
	if rc.Config.GitHubInstance != "github.com" {
		ghc.ServerURL = fmt.Sprintf("https://%s", rc.Config.GitHubInstance)
		ghc.APIURL = fmt.Sprintf("https://%s/api/v3", rc.Config.GitHubInstance)
		ghc.GraphQLURL = fmt.Sprintf("https://%s/api/graphql", rc.Config.GitHubInstance)
	}
	// allow to be overridden by user
	if rc.Config.Env["GITHUB_SERVER_URL"] != "" {
		ghc.ServerURL = rc.Config.Env["GITHUB_SERVER_URL"]
	}
	if rc.Config.Env["GITHUB_API_URL"] != "" {
		ghc.APIURL = rc.Config.Env["GITHUB_API_URL"]
	}
	if rc.Config.Env["GITHUB_GRAPHQL_URL"] != "" {
		ghc.GraphQLURL = rc.Config.Env["GITHUB_GRAPHQL_URL"]
	}

	return ghc
}

func isLocalCheckout(ghc *model.GithubContext, step *model.Step) bool {
	if step.Type() == model.StepTypeInvalid {
		// This will be errored out by the executor later, we need this here to avoid a null panic though
		return false
	}
	if step.Type() != model.StepTypeUsesActionRemote {
		return false
	}
	remoteAction := newRemoteAction(step.Uses)
	if remoteAction == nil {
		// IsCheckout() will nil panic if we dont bail out early
		return false
	}
	if !remoteAction.IsCheckout() {
		return false
	}

	if repository, ok := step.With["repository"]; ok && repository != ghc.Repository {
		return false
	}
	if repository, ok := step.With["ref"]; ok && repository != ghc.Ref {
		return false
	}
	return true
}

func nestedMapLookup(m map[string]interface{}, ks ...string) (rval interface{}) {
	var ok bool

	if len(ks) == 0 { // degenerate input
		return nil
	}
	if rval, ok = m[ks[0]]; !ok {
		return nil
	} else if len(ks) == 1 { // we've reached the final key
		return rval
	} else if m, ok = rval.(map[string]interface{}); !ok {
		return nil
	} else { // 1+ more keys
		return nestedMapLookup(m, ks[1:]...)
	}
}

func (rc *RunContext) withGithubEnv(ctx context.Context, github *model.GithubContext, env map[string]string) map[string]string {
	env["CI"] = "true"
	env["GITHUB_WORKFLOW"] = github.Workflow
	env["GITHUB_RUN_ID"] = github.RunID
	env["GITHUB_RUN_NUMBER"] = github.RunNumber
	env["GITHUB_ACTION"] = github.Action
	env["GITHUB_ACTION_PATH"] = github.ActionPath
	env["GITHUB_ACTION_REPOSITORY"] = github.ActionRepository
	env["GITHUB_ACTION_REF"] = github.ActionRef
	env["GITHUB_ACTIONS"] = "true"
	env["GITHUB_ACTOR"] = github.Actor
	env["GITHUB_REPOSITORY"] = github.Repository
	env["GITHUB_EVENT_NAME"] = github.EventName
	env["GITHUB_EVENT_PATH"] = github.EventPath
	env["GITHUB_WORKSPACE"] = github.Workspace
	env["GITHUB_SHA"] = github.Sha
	env["GITHUB_REF"] = github.Ref
	env["GITHUB_REF_NAME"] = github.RefName
	env["GITHUB_REF_TYPE"] = github.RefType
	env["GITHUB_TOKEN"] = github.Token
	env["GITHUB_JOB"] = github.Job
	env["GITHUB_REPOSITORY_OWNER"] = github.RepositoryOwner
	env["GITHUB_RETENTION_DAYS"] = github.RetentionDays
	env["RUNNER_PERFLOG"] = github.RunnerPerflog
	env["RUNNER_TRACKING_ID"] = github.RunnerTrackingID
	env["GITHUB_BASE_REF"] = github.BaseRef
	env["GITHUB_HEAD_REF"] = github.HeadRef
	env["GITHUB_SERVER_URL"] = github.ServerURL
	env["GITHUB_API_URL"] = github.APIURL
	env["GITHUB_GRAPHQL_URL"] = github.GraphQLURL

	{ // Adapt to Gitea
		instance := rc.Config.GitHubInstance
		if !strings.HasPrefix(instance, "http://") &&
			!strings.HasPrefix(instance, "https://") {
			instance = "https://" + instance
		}
		env["GITHUB_SERVER_URL"] = instance
		env["GITHUB_API_URL"] = instance + "/api/v1" // the version of Gitea is v1
		env["GITHUB_GRAPHQL_URL"] = ""               // Gitea doesn't support graphql
	}

	if rc.Config.ArtifactServerPath != "" {
		setActionRuntimeVars(rc, env)
	}

	job := rc.Run.Job()
	if job.RunsOn() != nil {
		for _, runnerLabel := range job.RunsOn() {
			platformName := rc.ExprEval.Interpolate(ctx, runnerLabel)
			if platformName != "" {
				if platformName == "ubuntu-latest" {
					// hardcode current ubuntu-latest since we have no way to check that 'on the fly'
					env["ImageOS"] = "ubuntu20"
				} else {
					platformName = strings.SplitN(strings.Replace(platformName, `-`, ``, 1), `.`, 2)[0]
					env["ImageOS"] = platformName
				}
			}
		}
	}

	return env
}

func setActionRuntimeVars(rc *RunContext, env map[string]string) {
	actionsRuntimeURL := os.Getenv("ACTIONS_RUNTIME_URL")
	if actionsRuntimeURL == "" {
		actionsRuntimeURL = fmt.Sprintf("http://%s:%s/", rc.Config.ArtifactServerAddr, rc.Config.ArtifactServerPort)
	}
	env["ACTIONS_RUNTIME_URL"] = actionsRuntimeURL

	actionsRuntimeToken := os.Getenv("ACTIONS_RUNTIME_TOKEN")
	if actionsRuntimeToken == "" {
		actionsRuntimeToken = "token"
	}
	env["ACTIONS_RUNTIME_TOKEN"] = actionsRuntimeToken
}

func (rc *RunContext) handleCredentials(ctx context.Context) (username, password string, err error) {
	// TODO: remove below 2 lines when we can release act with breaking changes
	username = rc.Config.Secrets["DOCKER_USERNAME"]
	password = rc.Config.Secrets["DOCKER_PASSWORD"]

	container := rc.Run.Job().Container()
	if container == nil || container.Credentials == nil {
		return
	}

	if container.Credentials != nil && len(container.Credentials) != 2 {
		err = fmt.Errorf("invalid property count for key 'credentials:'")
		return
	}

	ee := rc.NewExpressionEvaluator(ctx)
	if username = ee.Interpolate(ctx, container.Credentials["username"]); username == "" {
		err = fmt.Errorf("failed to interpolate container.credentials.username")
		return
	}
	if password = ee.Interpolate(ctx, container.Credentials["password"]); password == "" {
		err = fmt.Errorf("failed to interpolate container.credentials.password")
		return
	}

	if container.Credentials["username"] == "" || container.Credentials["password"] == "" {
		err = fmt.Errorf("container.credentials cannot be empty")
		return
	}

	return username, password, err
}

func (rc *RunContext) handleServiceCredentials(ctx context.Context, creds map[string]string) (username, password string, err error) {
	if creds == nil {
		return
	}
	if len(creds) != 2 {
		err = fmt.Errorf("invalid property count for key 'credentials:'")
		return
	}

	ee := rc.NewExpressionEvaluator(ctx)
	if username = ee.Interpolate(ctx, creds["username"]); username == "" {
		err = fmt.Errorf("failed to interpolate credentials.username")
		return
	}

	if password = ee.Interpolate(ctx, creds["password"]); password == "" {
		err = fmt.Errorf("failed to interpolate credentials.password")
		return
	}

	return
}
