package records

import (
	"fmt"

	"github.com/ceph/go-ceph/rados"
)

type RecordReader interface {
	ReadRecord(int) ([]Record, error)
	Peek() Record
	HasNext() bool
}

type CephRecordReader struct {
	ioctx   *rados.IOContext
	keys    []string
	preload Record
}

func NewCephRecordReader(key string) *CephRecordReader {
	args := []string{
		"--mon-host", "1.1.1.1",
	}
	conn, _ := rados.NewConn()
	err := conn.ParseCmdLineArgs(args)
	err = conn.Connect()
	ioctx, err := conn.OpenIOContext("ni")
	if err != nil {
		fmt.Println("OpenIOContext err: ", err)
	}
	return &CephRecordReader{
		ioctx:   ioctx,
		key:     key,
		preload: Record{},
	}
}

func (crr *CephRecordReader) ReadRecord(count int) ([]Record, error) {
}
