package executor

import (
	"sort"

	"github.com/naturali/kmr/job"
	"github.com/naturali/kmr/records"
)

type Executor interface {
}

type MapperWrap struct {
}

type ReducerWrap struct {
}

func (mw *MapperWrap) Map(rr records.RecordReader, ctx job.Context) {
	aggregated := []records.Record{}
	for {
		// FIXME: 64k
		items, err := rr.ReadRecord(1 << 16)
		if err != nil {
		}
		if len(items) == 0 {
			break
		}
		// TODO: grpc call compute
		//
		mapResult := []records.Record{}
		//aggregated = append(aggregated, mapResult...)
		ctx.Write(aggregated)
	}
	sort.Sort(ByKey(aggregated))
	//ctx.Write(aggregated)
}

func (rw *ReducerWrap) Reduce(rr records.RecordReader, ctx job.Context) {
	pre := records.Record{}
	aggregated := []records.Record{}
	rst := []records.Record{}

	for {
		// FIXME: 64k
		items, err := rr.ReadRecord(1 << 16)
		if err != nil {
		}
		if len(items) == 0 {
			break
		}

		for item := range items {
			if pre.Key == "" {
				pre = item
			} else {
				if item.Key != pre.Key {
					// grpc call compute with aggregated
					reduceResult := []records.Record{}
					rst = append(rst, reduceResult...)
				} else {
					aggregated = append(aggregated, item)
				}
			}

		}
	}
	ctx.Write(rst)
}
