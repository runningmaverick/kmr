package main

import (
	"flag"
	"os"

	"github.com/ceph/go-ceph/rados"
	"github.com/naturali/NiServer/pkg/log"

	"github.com/naturali/kmr/job"
)

func main() {
	fs := flag.NewFlagSet("name", flag.ExitOnError)
	shards := fs.String("shard1,shard2,shard3", "key1", "")
	if err := fs.Parse(os.Args[1:]); err != nil {
		log.Fatal(err)
	}

	mapper := executor.MapperWrap{}

	shards := strings.Split(*shards, ",")
	for shard := range shards {
		reader := records.NewCephRecordReader(shard)
		mapper.Map(reader, job.Context{})
	}

	log.Info("task completed")
}
