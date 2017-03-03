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
	key := fs.String("ceph-key", "key1", "")
	if err := fs.Parse(os.Args[1:]); err != nil {
		log.Fatal(err)
	}

	reducer := executor.ReducerWrap{}
	reader := records.NewCephRecordReader(key)
	reducer.Reduce(reader, job.Context{})

	log.Info("task completed")
}
