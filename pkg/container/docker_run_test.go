package container

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/nektos/act/pkg/common"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestDocker(t *testing.T) {
	ctx := context.Background()
	client, err := GetDockerClient(ctx)
	assert.NoError(t, err)
	defer client.Close()

	dockerBuild := NewDockerBuildExecutor(NewDockerBuildExecutorInput{
		ContextDir: "testdata",
		ImageTag:   "envmergetest",
	})

	err = dockerBuild(ctx)
	assert.NoError(t, err)

	cr := &containerReference{
		cli: client,
		input: &NewContainerInput{
			Image: "envmergetest",
		},
	}
	env := map[string]string{
		"PATH":         "/usr/local/bin:/usr/bin:/usr/sbin:/bin:/sbin",
		"RANDOM_VAR":   "WITH_VALUE",
		"ANOTHER_VAR":  "",
		"CONFLICT_VAR": "I_EXIST_IN_MULTIPLE_PLACES",
	}

	envExecutor := cr.extractFromImageEnv(&env)
	err = envExecutor(ctx)
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{
		"PATH":            "/usr/local/bin:/usr/bin:/usr/sbin:/bin:/sbin:/this/path/does/not/exists/anywhere:/this/either",
		"RANDOM_VAR":      "WITH_VALUE",
		"ANOTHER_VAR":     "",
		"SOME_RANDOM_VAR": "",
		"ANOTHER_ONE":     "BUT_I_HAVE_VALUE",
		"CONFLICT_VAR":    "I_EXIST_IN_MULTIPLE_PLACES",
	}, env)
}

type mockDockerClient struct {
	client.APIClient
	mock.Mock
}

func (m *mockDockerClient) ExecCreate(ctx context.Context, id string, opts client.ExecCreateOptions) (client.ExecCreateResult, error) {
	args := m.Called(ctx, id, opts)
	return args.Get(0).(client.ExecCreateResult), args.Error(1)
}

func (m *mockDockerClient) ExecAttach(ctx context.Context, id string, opts client.ExecAttachOptions) (client.ExecAttachResult, error) {
	args := m.Called(ctx, id, opts)
	return args.Get(0).(client.ExecAttachResult), args.Error(1)
}

func (m *mockDockerClient) ExecInspect(ctx context.Context, execID string, opts client.ExecInspectOptions) (client.ExecInspectResult, error) {
	args := m.Called(ctx, execID, opts)
	return args.Get(0).(client.ExecInspectResult), args.Error(1)
}

func (m *mockDockerClient) CopyToContainer(ctx context.Context, id string, options client.CopyToContainerOptions) (client.CopyToContainerResult, error) {
	args := m.Called(ctx, id, options.DestinationPath, options.Content, options)
	return client.CopyToContainerResult{}, args.Error(0)
}

type endlessReader struct {
	io.Reader
}

func (r endlessReader) Read(_ []byte) (n int, err error) {
	return 1, nil
}

type mockConn struct {
	net.Conn
	mock.Mock
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	args := m.Called(b)
	return args.Int(0), args.Error(1)
}

func (m *mockConn) Close() (err error) {
	return nil
}

func TestDockerExecAbort(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	conn := &mockConn{}
	conn.On("Write", mock.AnythingOfType("[]uint8")).Return(1, nil)

	cli := &mockDockerClient{}
	cli.On("ExecCreate", ctx, "123", mock.AnythingOfType("client.ExecCreateOptions")).Return(client.ExecCreateResult{ID: "id"}, nil)
	cli.On("ExecAttach", ctx, "id", mock.AnythingOfType("client.ExecAttachOptions")).Return(client.ExecAttachResult{
		HijackedResponse: client.HijackedResponse{
			Conn:   conn,
			Reader: bufio.NewReader(endlessReader{}),
		},
	}, nil)

	cr := &containerReference{
		id:  "123",
		cli: cli,
		input: &NewContainerInput{
			Image: "image",
		},
	}

	channel := make(chan error)

	go func() {
		channel <- cr.exec([]string{""}, map[string]string{}, "user", "workdir")(ctx)
	}()

	time.Sleep(500 * time.Millisecond)

	cancel()

	err := <-channel
	assert.ErrorIs(t, err, context.Canceled)

	conn.AssertExpectations(t)
	cli.AssertExpectations(t)
}

func TestDockerExecFailure(t *testing.T) {
	ctx := context.Background()

	conn := &mockConn{}

	cli := &mockDockerClient{}
	cli.On("ExecCreate", ctx, "123", mock.AnythingOfType("client.ExecCreateOptions")).Return(client.ExecCreateResult{ID: "id"}, nil)
	cli.On("ExecAttach", ctx, "id", mock.AnythingOfType("client.ExecAttachOptions")).Return(client.ExecAttachResult{
		HijackedResponse: client.HijackedResponse{
			Conn:   conn,
			Reader: bufio.NewReader(strings.NewReader("output")),
		},
	}, nil)
	cli.On("ExecInspect", ctx, "id", mock.AnythingOfType("client.ExecInspectOptions")).Return(client.ExecInspectResult{
		ExitCode: 1,
	}, nil)

	cr := &containerReference{
		id:  "123",
		cli: cli,
		input: &NewContainerInput{
			Image: "image",
		},
	}

	err := cr.exec([]string{""}, map[string]string{}, "user", "workdir")(ctx)
	assert.Error(t, err, "exit with `FAILURE`: 1")

	conn.AssertExpectations(t)
	cli.AssertExpectations(t)
}

func TestDockerCopyTarStream(t *testing.T) {
	ctx := context.Background()

	conn := &mockConn{}

	client := &mockDockerClient{}
	client.On("CopyToContainer", ctx, "123", "/", mock.Anything, mock.AnythingOfType("client.CopyToContainerOptions")).Return(nil)
	client.On("CopyToContainer", ctx, "123", "/var/run/act", mock.Anything, mock.AnythingOfType("client.CopyToContainerOptions")).Return(nil)
	cr := &containerReference{
		id:  "123",
		cli: client,
		input: &NewContainerInput{
			Image: "image",
		},
	}

	_ = cr.CopyTarStream(ctx, "/var/run/act", &bytes.Buffer{})

	conn.AssertExpectations(t)
	client.AssertExpectations(t)
}

func TestDockerCopyTarStreamErrorInCopyFiles(t *testing.T) {
	ctx := context.Background()

	conn := &mockConn{}

	merr := fmt.Errorf("Failure")

	client := &mockDockerClient{}
	client.On("CopyToContainer", ctx, "123", "/", mock.Anything, mock.AnythingOfType("client.CopyToContainerOptions")).Return(merr)
	client.On("CopyToContainer", ctx, "123", "/", mock.Anything, mock.AnythingOfType("client.CopyToContainerOptions")).Return(merr)
	cr := &containerReference{
		id:  "123",
		cli: client,
		input: &NewContainerInput{
			Image: "image",
		},
	}

	err := cr.CopyTarStream(ctx, "/var/run/act", &bytes.Buffer{})
	assert.ErrorIs(t, err, merr)

	conn.AssertExpectations(t)
	client.AssertExpectations(t)
}

func TestDockerCopyTarStreamErrorInMkdir(t *testing.T) {
	ctx := context.Background()

	conn := &mockConn{}

	merr := fmt.Errorf("Failure")

	client := &mockDockerClient{}
	client.On("CopyToContainer", ctx, "123", "/", mock.Anything, mock.AnythingOfType("client.CopyToContainerOptions")).Return(nil)
	client.On("CopyToContainer", ctx, "123", "/var/run/act", mock.Anything, mock.AnythingOfType("client.CopyToContainerOptions")).Return(merr)
	cr := &containerReference{
		id:  "123",
		cli: client,
		input: &NewContainerInput{
			Image: "image",
		},
	}

	err := cr.CopyTarStream(ctx, "/var/run/act", &bytes.Buffer{})
	assert.ErrorIs(t, err, merr)

	conn.AssertExpectations(t)
	client.AssertExpectations(t)
}

// Type assert containerReference implements ExecutionsEnvironment
var _ ExecutionsEnvironment = &containerReference{}

func TestCheckVolumes(t *testing.T) {
	testCases := []struct {
		desc           string
		validVolumes   []string
		binds          []string
		mounts         []mount.Mount
		expectedBinds  []string
		expectedMounts []mount.Mount
	}{
		{
			desc:         "match all volumes",
			validVolumes: []string{"**"},
			binds: []string{
				"shared_volume:/shared_volume",
				"/home/test/data:/test_data",
				"/etc/conf.d/base.json:/config/base.json",
				"sql_data:/sql_data",
				"/secrets/keys:/keys",
			},
			expectedBinds: []string{
				"shared_volume:/shared_volume",
				"/home/test/data:/test_data",
				"/etc/conf.d/base.json:/config/base.json",
				"sql_data:/sql_data",
				"/secrets/keys:/keys",
			},
		},
		{
			desc:         "no volumes can be matched",
			validVolumes: []string{},
			binds: []string{
				"shared_volume:/shared_volume",
				"/home/test/data:/test_data",
				"/etc/conf.d/base.json:/config/base.json",
				"sql_data:/sql_data",
				"/secrets/keys:/keys",
			},
			expectedBinds: []string{},
		},
		{
			desc: "only allowed volumes can be matched",
			validVolumes: []string{
				"shared_volume",
				"/home/test/data",
				"/etc/conf.d/*.json",
			},
			binds: []string{
				"shared_volume:/shared_volume",
				"/home/test/data:/test_data",
				"/etc/conf.d/base.json:/config/base.json",
				"sql_data:/sql_data",
				"/secrets/keys:/keys",
			},
			expectedBinds: []string{
				"shared_volume:/shared_volume",
				"/home/test/data:/test_data",
				"/etc/conf.d/base.json:/config/base.json",
			},
		},
		{
			desc:          "read-only entry forces a bind read-only",
			validVolumes:  []string{"/etc/ssl/certs/ca-certificates.crt:ro"},
			binds:         []string{"/etc/ssl/certs/ca-certificates.crt:/etc/ssl/certs/ca-certificates.crt"},
			expectedBinds: []string{"/etc/ssl/certs/ca-certificates.crt:/etc/ssl/certs/ca-certificates.crt:ro"},
		},
		{
			desc:          "read-only entry rewrites a requested rw mode",
			validVolumes:  []string{"/host/toolcache:ro"},
			binds:         []string{"/host/toolcache:/opt/hostedtoolcache:rw"},
			expectedBinds: []string{"/host/toolcache:/opt/hostedtoolcache:ro"},
		},
		{
			desc:          "unmarked entry preserves the requested rw mode",
			validVolumes:  []string{"/home/test/data"},
			binds:         []string{"/home/test/data:/test_data:rw"},
			expectedBinds: []string{"/home/test/data:/test_data:rw"},
		},
		{
			desc:          "already read-only bind stays read-only",
			validVolumes:  []string{"/home/test/data:ro"},
			binds:         []string{"/home/test/data:/test_data:ro"},
			expectedBinds: []string{"/home/test/data:/test_data:ro"},
		},
		{
			desc:          "read-only entry keeps other bind modifiers",
			validVolumes:  []string{"/home/test/data:ro"},
			binds:         []string{"/home/test/data:/test_data:z"},
			expectedBinds: []string{"/home/test/data:/test_data:z,ro"},
		},
		{
			desc:          "read-only entry handles Windows drive paths without a mode",
			validVolumes:  []string{"C:/host/data:ro"},
			binds:         []string{"C:/host/data:D:/container/data"},
			expectedBinds: []string{"C:/host/data:D:/container/data:ro"},
		},
		{
			desc:          "non-matching bind is dropped when read-only rules exist",
			validVolumes:  []string{"/home/test/data:ro"},
			binds:         []string{"/secrets/keys:/keys"},
			expectedBinds: []string{},
		},
		{
			desc: "last matching rule wins for overlapping entries",
			validVolumes: []string{
				"/data/**",
				"/data/cache:ro",
			},
			binds: []string{
				"/data/cache:/cache",
				"/data/tool:/tool",
			},
			expectedBinds: []string{
				"/data/cache:/cache:ro",
				"/data/tool:/tool",
			},
		},
		{
			desc: "internally appended exact entry stays writable",
			validVolumes: []string{
				"/**:ro",
				"/var/run/docker.sock",
			},
			binds: []string{
				"/var/run/docker.sock:/var/run/docker.sock",
				"/etc/passwd:/pw",
			},
			expectedBinds: []string{
				"/var/run/docker.sock:/var/run/docker.sock",
				"/etc/passwd:/pw:ro",
			},
		},
		{
			desc:         "read-only entry forces a long-form mount read-only",
			validVolumes: []string{"/etc/ssl/certs/ca-certificates.crt:ro"},
			mounts: []mount.Mount{
				{
					Type:   mount.TypeBind,
					Source: "/etc/ssl/certs/ca-certificates.crt",
					Target: "/etc/ssl/certs/ca-certificates.crt",
				},
			},
			expectedMounts: []mount.Mount{
				{
					Type:     mount.TypeBind,
					Source:   "/etc/ssl/certs/ca-certificates.crt",
					Target:   "/etc/ssl/certs/ca-certificates.crt",
					ReadOnly: true,
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			ctx := common.WithLogger(context.Background(), logger)
			cr := &containerReference{
				input: &NewContainerInput{
					ValidVolumes: tc.validVolumes,
				},
			}
			_, hostConf := cr.sanitizeConfig(ctx, &container.Config{}, &container.HostConfig{Binds: tc.binds, Mounts: tc.mounts})
			if tc.expectedBinds == nil {
				tc.expectedBinds = []string{}
			}
			if tc.expectedMounts == nil {
				tc.expectedMounts = []mount.Mount{}
			}
			assert.Equal(t, tc.expectedBinds, hostConf.Binds)
			assert.Equal(t, tc.expectedMounts, hostConf.Mounts)
		})
	}
}

func TestOptionsMountSurvivesSanitize(t *testing.T) {
	logger, _ := test.NewNullLogger()
	ctx := common.WithLogger(context.Background(), logger)

	source := "/etc/ssl/certs/ca-certificates.crt"
	cr := &containerReference{
		input: &NewContainerInput{
			Options:      fmt.Sprintf("--mount type=bind,source=%s,target=%s,readonly", source, source),
			NetworkMode:  "container:job",
			ValidVolumes: []string{source},
		},
	}

	config, hostConfig, err := cr.mergeContainerConfigs(ctx, &container.Config{}, &container.HostConfig{})
	require.NoError(t, err)

	_, hostConfig = cr.sanitizeConfig(ctx, config, hostConfig)

	assert.Len(t, hostConfig.Mounts, 1)
	if len(hostConfig.Mounts) == 1 {
		m := hostConfig.Mounts[0]
		assert.Equal(t, mount.TypeBind, m.Type)
		assert.Equal(t, source, m.Source)
		assert.Equal(t, source, m.Target)
		assert.True(t, m.ReadOnly)
	}
}
