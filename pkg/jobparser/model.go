package jobparser

import (
	"fmt"

	"github.com/nektos/act/pkg/model"
	"gopkg.in/yaml.v3"
)

// SingleWorkflow is a workflow with single job and single matrix
type SingleWorkflow struct {
	Name     string            `yaml:"name,omitempty"`
	RawOn    yaml.Node         `yaml:"on,omitempty"`
	Env      map[string]string `yaml:"env,omitempty"`
	RawJobs  yaml.Node         `yaml:"jobs,omitempty"`
	Defaults Defaults          `yaml:"defaults,omitempty"`
}

func (w *SingleWorkflow) Job() (string, *Job) {
	ids, jobs, _ := w.jobs()
	if len(ids) >= 1 {
		return ids[0], jobs[0]
	}
	return "", nil
}

func (w *SingleWorkflow) jobs() ([]string, []*Job, error) {
	ids, jobs, err := parseMappingNode[*Job](&w.RawJobs)
	if err != nil {
		return nil, nil, err
	}

	for _, job := range jobs {
		steps := make([]*Step, 0, len(job.Steps))
		for _, s := range job.Steps {
			if s != nil {
				steps = append(steps, s)
			}
		}
		job.Steps = steps
	}

	return ids, jobs, nil
}

func (w *SingleWorkflow) SetJob(id string, job *Job) error {
	m := map[string]*Job{
		id: job,
	}
	out, err := yaml.Marshal(m)
	if err != nil {
		return err
	}
	node := yaml.Node{}
	if err := yaml.Unmarshal(out, &node); err != nil {
		return err
	}
	if len(node.Content) != 1 || node.Content[0].Kind != yaml.MappingNode {
		return fmt.Errorf("can not set job: %q", out)
	}
	w.RawJobs = *node.Content[0]
	return nil
}

func (w *SingleWorkflow) Marshal() ([]byte, error) {
	return yaml.Marshal(w)
}

type Job struct {
	Name           string                    `yaml:"name,omitempty"`
	RawNeeds       yaml.Node                 `yaml:"needs,omitempty"`
	RawRunsOn      yaml.Node                 `yaml:"runs-on,omitempty"`
	Env            yaml.Node                 `yaml:"env,omitempty"`
	If             yaml.Node                 `yaml:"if,omitempty"`
	Steps          []*Step                   `yaml:"steps,omitempty"`
	TimeoutMinutes string                    `yaml:"timeout-minutes,omitempty"`
	Services       map[string]*ContainerSpec `yaml:"services,omitempty"`
	Strategy       Strategy                  `yaml:"strategy,omitempty"`
	RawContainer   yaml.Node                 `yaml:"container,omitempty"`
	Defaults       Defaults                  `yaml:"defaults,omitempty"`
	Outputs        map[string]string         `yaml:"outputs,omitempty"`
	Uses           string                    `yaml:"uses,omitempty"`
	With           map[string]interface{}    `yaml:"with,omitempty"`
	RawSecrets     yaml.Node                 `yaml:"secrets,omitempty"`
}

func (j *Job) Clone() *Job {
	if j == nil {
		return nil
	}
	return &Job{
		Name:           j.Name,
		RawNeeds:       j.RawNeeds,
		RawRunsOn:      j.RawRunsOn,
		Env:            j.Env,
		If:             j.If,
		Steps:          j.Steps,
		TimeoutMinutes: j.TimeoutMinutes,
		Services:       j.Services,
		Strategy:       j.Strategy,
		RawContainer:   j.RawContainer,
		Defaults:       j.Defaults,
		Outputs:        j.Outputs,
		Uses:           j.Uses,
		With:           j.With,
		RawSecrets:     j.RawSecrets,
	}
}

func (j *Job) Needs() []string {
	return (&model.Job{RawNeeds: j.RawNeeds}).Needs()
}

func (j *Job) EraseNeeds() *Job {
	j.RawNeeds = yaml.Node{}
	return j
}

func (j *Job) RunsOn() []string {
	return (&model.Job{RawRunsOn: j.RawRunsOn}).RunsOn()
}

type Step struct {
	ID               string            `yaml:"id,omitempty"`
	If               yaml.Node         `yaml:"if,omitempty"`
	Name             string            `yaml:"name,omitempty"`
	Uses             string            `yaml:"uses,omitempty"`
	Run              string            `yaml:"run,omitempty"`
	WorkingDirectory string            `yaml:"working-directory,omitempty"`
	Shell            string            `yaml:"shell,omitempty"`
	Env              yaml.Node         `yaml:"env,omitempty"`
	With             map[string]string `yaml:"with,omitempty"`
	ContinueOnError  bool              `yaml:"continue-on-error,omitempty"`
	TimeoutMinutes   string            `yaml:"timeout-minutes,omitempty"`
}

// String gets the name of step
func (s *Step) String() string {
	if s == nil {
		return ""
	}
	return (&model.Step{
		ID:   s.ID,
		Name: s.Name,
		Uses: s.Uses,
		Run:  s.Run,
	}).String()
}

type ContainerSpec struct {
	Image       string            `yaml:"image,omitempty"`
	Env         map[string]string `yaml:"env,omitempty"`
	Ports       []string          `yaml:"ports,omitempty"`
	Volumes     []string          `yaml:"volumes,omitempty"`
	Options     string            `yaml:"options,omitempty"`
	Credentials map[string]string `yaml:"credentials,omitempty"`
	Cmd         []string          `yaml:"cmd,omitempty"`
}

type Strategy struct {
	FailFastString    string    `yaml:"fail-fast,omitempty"`
	MaxParallelString string    `yaml:"max-parallel,omitempty"`
	RawMatrix         yaml.Node `yaml:"matrix,omitempty"`
}

type Defaults struct {
	Run RunDefaults `yaml:"run,omitempty"`
}

type RunDefaults struct {
	Shell            string `yaml:"shell,omitempty"`
	WorkingDirectory string `yaml:"working-directory,omitempty"`
}

type WorkflowDispatchInput struct {
	Name        string   `yaml:"name"`
	Description string   `yaml:"description"`
	Required    bool     `yaml:"required"`
	Default     string   `yaml:"default"`
	Type        string   `yaml:"type"`
	Options     []string `yaml:"options"`
}

type Event struct {
	Name      string
	acts      map[string][]string
	schedules []map[string]string
	inputs    []WorkflowDispatchInput
}

func (evt *Event) IsSchedule() bool {
	return evt.schedules != nil
}

func (evt *Event) Acts() map[string][]string {
	return evt.acts
}

func (evt *Event) Schedules() []map[string]string {
	return evt.schedules
}

func (evt *Event) Inputs() []WorkflowDispatchInput {
	return evt.inputs
}

func parseWorkflowDispatchInputs(inputs map[string]interface{}) ([]WorkflowDispatchInput, error) {
	var results []WorkflowDispatchInput
	for name, input := range inputs {
		inputMap, ok := input.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid input: %v", input)
		}
		input := WorkflowDispatchInput{
			Name: name,
		}
		if desc, ok := inputMap["description"].(string); ok {
			input.Description = desc
		}
		if required, ok := inputMap["required"].(bool); ok {
			input.Required = required
		}
		if defaultVal, ok := inputMap["default"].(string); ok {
			input.Default = defaultVal
		}
		if inputType, ok := inputMap["type"].(string); ok {
			input.Type = inputType
		}
		if options, ok := inputMap["options"].([]string); ok {
			input.Options = options
		} else if options, ok := inputMap["options"].([]interface{}); ok {
			for _, option := range options {
				if opt, ok := option.(string); ok {
					input.Options = append(input.Options, opt)
				}
			}
		}

		results = append(results, input)
	}
	return results, nil
}

func ParseRawOn(rawOn *yaml.Node) ([]*Event, error) {
	switch rawOn.Kind {
	case yaml.ScalarNode:
		var val string
		err := rawOn.Decode(&val)
		if err != nil {
			return nil, err
		}
		return []*Event{
			{Name: val},
		}, nil
	case yaml.SequenceNode:
		var val []interface{}
		err := rawOn.Decode(&val)
		if err != nil {
			return nil, err
		}
		res := make([]*Event, 0, len(val))
		for _, v := range val {
			switch t := v.(type) {
			case string:
				res = append(res, &Event{Name: t})
			default:
				return nil, fmt.Errorf("invalid type %T", t)
			}
		}
		return res, nil
	case yaml.MappingNode:
		events, triggers, err := parseMappingNode[yaml.Node](rawOn)
		if err != nil {
			return nil, err
		}
		res := make([]*Event, 0, len(events))
		for i, k := range events {
			v := triggers[i]
			switch v.Kind {
			case yaml.ScalarNode:
				res = append(res, &Event{
					Name: k,
				})
			case yaml.SequenceNode:
				var t []interface{}
				err := v.Decode(&t)
				if err != nil {
					return nil, err
				}
				schedules := make([]map[string]string, len(t))
				if k == "schedule" {
					for i, tt := range t {
						vv, ok := tt.(map[string]interface{})
						if !ok {
							return nil, fmt.Errorf("unknown on type(schedule): %#v", v)
						}
						schedules[i] = make(map[string]string, len(vv))
						for k, vvv := range vv {
							var ok bool
							if schedules[i][k], ok = vvv.(string); !ok {
								return nil, fmt.Errorf("unknown on type(schedule): %#v", v)
							}
						}
					}
				}

				if len(schedules) == 0 {
					schedules = nil
				}
				res = append(res, &Event{
					Name:      k,
					schedules: schedules,
				})
			case yaml.MappingNode:
				acts := make(map[string][]string, len(v.Content)/2)
				var inputs []WorkflowDispatchInput
				expectedKey := true
				var act string
				for _, content := range v.Content {
					if expectedKey {
						if content.Kind != yaml.ScalarNode {
							return nil, fmt.Errorf("key type not string: %#v", content)
						}
						act = ""
						err := content.Decode(&act)
						if err != nil {
							return nil, err
						}
					} else {
						switch content.Kind {
						case yaml.SequenceNode:
							var t []string
							err := content.Decode(&t)
							if err != nil {
								return nil, err
							}
							acts[act] = t
						case yaml.MappingNode:
							if k != "workflow_dispatch" || act != "inputs" {
								return nil, fmt.Errorf("map should only for workflow_dispatch but %s: %#v", act, content)
							}

							var key string
							for i, vv := range content.Content {
								if i%2 == 0 {
									if vv.Kind != yaml.ScalarNode {
										return nil, fmt.Errorf("key type not string: %#v", vv)
									}
									key = ""
									if err := vv.Decode(&key); err != nil {
										return nil, err
									}
								} else {
									if vv.Kind != yaml.MappingNode {
										return nil, fmt.Errorf("key type not map(%s): %#v", key, vv)
									}

									input := WorkflowDispatchInput{}
									if err := vv.Decode(&input); err != nil {
										return nil, err
									}
									input.Name = key
									inputs = append(inputs, input)
								}
							}
						default:
							return nil, fmt.Errorf("unknown on type: %#v", content)
						}
					}
					expectedKey = !expectedKey
				}
				if len(inputs) == 0 {
					inputs = nil
				}
				if len(acts) == 0 {
					acts = nil
				}
				res = append(res, &Event{
					Name:   k,
					acts:   acts,
					inputs: inputs,
				})
			default:
				return nil, fmt.Errorf("unknown on type: %v", v.Kind)
			}
		}
		return res, nil
	default:
		return nil, fmt.Errorf("unknown on type: %v", rawOn.Kind)
	}
}

// parseMappingNode parse a mapping node and preserve order.
func parseMappingNode[T any](node *yaml.Node) ([]string, []T, error) {
	if node.Kind != yaml.MappingNode {
		return nil, nil, fmt.Errorf("input node is not a mapping node")
	}

	var scalars []string
	var datas []T
	expectKey := true
	for _, item := range node.Content {
		if expectKey {
			if item.Kind != yaml.ScalarNode {
				return nil, nil, fmt.Errorf("not a valid scalar node: %v", item.Value)
			}
			scalars = append(scalars, item.Value)
			expectKey = false
		} else {
			var val T
			if err := item.Decode(&val); err != nil {
				return nil, nil, err
			}
			datas = append(datas, val)
			expectKey = true
		}
	}

	if len(scalars) != len(datas) {
		return nil, nil, fmt.Errorf("invalid definition of on: %v", node.Value)
	}

	return scalars, datas, nil
}
