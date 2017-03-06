package main

import (
	"flag"
	"log"
	"os"

	"github.com/ceph/go-ceph/rados"

	"github.com/naturali/kmr/job"
	"github.com/naturali/kmr/records"
)

func main() {
	fs := flag.NewFlagSet("name", flag.ExitOnError)
	jobName := fs.String("job-name", "", "")
	mapperID := fs.String("mapper-id", "", "")

	shards := fs.String("shards", "file1,file2,file3", "")
	readerClass := fs.String("reader-class", "file", "file|console|obj")
	writerClass := fs.String("writer-class", "file", "")
	shardCount := fs.Int("share-count", 1, "")
	mapperCount := fs.Int("mapper-count", 1, "")
	reducerCount := fs.Int("reducer-count", 1, "")

	ctx := job.GetContext(
		job.Task{
			Phase: "map",
		}, job.JobConfig{
			shardCount:   shardCount,
			mapperCount:  mapperCount,
			reducerCount: reducerCount,

			readerClass: readerClass,
			writerClass: writerClass,
		})

	if err := fs.Parse(os.Args[1:]); err != nil {
		log.Fatal(err)
	}

	mapper := executor.MapperWrap{}

	shards := strings.Split(*shards, ",")
	for shard := range shards {
		reader := records.MakeRecordReader(*readerClass, map[string]string{"filename": shard})
		mapper.Map(reader, ctx)
	}

	log.Info("task completed")
}
