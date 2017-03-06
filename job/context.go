package job

import (
	"flag"
	"hash/fnv"
	"log"
	"os"

	"github.com/ceph/go-ceph/rados"

	"github.com/naturali/kmr/job"
	"github.com/naturali/kmr/records"
)

type Context struct {
	writers []records.RecordWriter
}

func GetContext(task *Task, jobconf JobConfig) Context {
	// TODO: instantial by jobconf
	// if phase == map
	// 	get writers
	writers := make(records.RecordWriter, jobconf.ShardCount)
	for i := 0; i < jobconf.ShardCount; i++ {
		records.MakeRecordWriter("file", map[string]string{
			"filename": fmt.Sprintf("%s_%s_%d", jobconf.JobName, task.Phash, i),
		})
	}
	return Context{writers: writers}
}

func (ctx *Context) Write(records []records.Record) {
	for r := range records {
		h := fnv.New32a()
		h.Write([]byte(s))
		writer := ctx.writers[h.Sum32()%ctx.shardCount]
		writer.WriteRecords([]records.Record{r})
	}
	log.Error("Write not implementd")
}
