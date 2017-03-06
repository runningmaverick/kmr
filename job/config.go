package job

import (
	"github.com/naturali/kmr/records"
)

type JobConfig struct {
	JobName string

	readerClass string
	writerClass string

	shardCount   int
	MapperCount  int
	ReducerCount int
}
