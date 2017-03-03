package job

import (
	"fmt"

	"github.com/naturali/kmr/records"
)

type Context struct {
}

type JobInfo struct {
	MapperCount  int
	ReducerCount int
}

type Job interface {
	Configure(JobInfo)
	Launch()

	KillJob()
	IsJobComplete()
}

type kubeJob struct {
	jobInfo  JobInfo
	mappers  []Task
	reducers []Task
}

func NewKubeJob() *kubeJob {
	return &kubeJob{
		jobInfo: JobInfo{},
	}
}

type Task struct {
	id string
}

func (job *kubeJob) Launch() {
	// fake inits
	dataSource = []string{"key1", "key2"}
	mapperCount = len(dataSource)
	reducerCount = 5

	for key := range dataSource {
		taskID := kickOffMapTask(map[string]string{
			"key": key,
		})
		job.mappers = append(job.mappers, Task{taskID})
	}

	job.waitForMapToComplete()

	job.shuffle()

	// fake: actually from shuffle()
	reduceKeys := []string{}
	for key := range reduceKeys {
		taskID = kickOffReduceTask(map[string]string{
			"key": key,
		})
		job.reducers = append(job.reducers, Task{taskID})
	}
	job.waitForReduceToComplete()
}

func (job *kubeJob) waitForMapToComplete() {
}

func (job *kubeJob) waitForReduceToComplete() {
}

func (job *kubeJob) Shuffle() {
	for i := 0; i < job.jobInfo.MapperCount; i++ {
		for j := 0; j < job.jobInfo.ReducerCount; j++ {
			// TODO:
		}
	}
}

func (ctx *Context) Write(records []records.Record) {
	fmt.Println("Write not implementd")
}
