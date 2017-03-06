package job

import (
	"log"

	"github.com/naturali/kmr/records"
)

type Job interface {
	Configure(JobConfig)
	Launch()
	Abort()

	GetStatus()
	IsJobComplete()
}

type kubeJob struct {
	jobConfig JobConfig
	mappers   []Task
	reducers  []Task
}

func NewKubeJob() *kubeJob {
	return &kubeJob{
		jobConfig: JobConfig{},
	}
}

type Task struct {
	id    string
	Phase string
}

func (job *kubeJob) Launch() {
	// fake inits
	dataSource := []string{"file1, file2", "file3,file4", "file5"}
	mapperCount := len(dataSource)
	reducerCount := 5

	for shards := range dataSource {
		task := &Task{
			phase:   "map",
			handler: "name.of.handler",
			shards:  shards,
		}
		kickOffTask(task)

		job.mappers = append(job.mappers, task)
	}

	job.waitForMapToComplete()

	job.shuffle()

	// fake: from shuffle()
	reduceKeys := []string{}
	for key := range reduceKeys {
		task := &Task{
			phase:   "reduce",
			handler: "name.of.handler",
			shards:  shards,
		}
		kickOffask(task)
		job.reducers = append(job.reducers, Task{taskID})
	}
	job.waitForReduceToComplete()
}

func (job *kubeJob) waitForMapToComplete() {
}

func (job *kubeJob) waitForReduceToComplete() {
}

func (job *kubeJob) Shuffle() {
	for i := 0; i < job.jobConfig.MapperCount; i++ {
		for j := 0; j < job.jobConfig.ReducerCount; j++ {
			// TODO:
		}
	}
}

func (job *kubeJob) kickOffTask(task *Task) {
	// TODO: submit a k8s job
}
