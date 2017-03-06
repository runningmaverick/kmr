package records

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/ceph/go-ceph/rados"
)

type RecordReader interface {
	Peek() Record
	HasNext() bool

	Iter() <-chan Record
}

type SimpleRecordReader struct {
	preload chan Record
}

type CephRecordReader struct {
	keys    []string
	preload chan Record
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

	preload := make(chan Record, 1000)
	go func() {
		buff := make([]byte, 5)
		var offset uint64 = 0
		for {
			n, err := ioctx.Read("obj", buff, offset)
			if err != nil {
				fmt.Printf("Cannot write %s, err: %v\n", "obj", err)
				break
			}
			if n > 0 {
				offset += uint64(n)
				// TODO: parse and push to preload
			} else {
				fmt.Printf("EOF")
				break
			}
			fmt.Printf("read count: %d, content: %s\n", n, buff[:n])
		}
		close(preload)
	}()

	return &CephRecordReader{
		key:     key,
		preload: preload,
	}
}

func (crr *CephRecordReader) Iter() <-chan Record {
	return crr.preload
}

func (srr *SimpleRecordReader) Iter() <-chan Record {
	return srr.preload
}

func NewConsoleRecordReader() *SimpleRecordReader {
	reader := bufio.NewReader(os.Stdin)
	preload := make(chan Record, 1000)

	feedStream(preload, reader)

	return &SimpleRecordReader{
		preload: preload,
	}
}

func NewFileRecordReader(filename string) *SimpleRecordReader {
	file, err := os.Open(filename)
	if err != nil {
		panic("fail to create file reader")
	}
	reader := bufio.NewReader(file)
	preload := make(chan Record, 1000)

	feedStream(preload, reader)

	return &SimpleRecordReader{
		preload: preload,
	}
}

func feedStream(preload <-chan Record, reader bufio.Reader) {
	go func() {
		for {
			text, err = reader.ReadString("\n")
			if err != nil {
				fmt.Printf("Cannot read %s, err: %v\n", err)
				break
			}
			fmt.Printf("read line: %s\n", text)

			// TODO:
			vals := strings.SplitN(text, " ", 2)
			if len(vals) == 2 {
				preload <- Record{vals[0], vals[1]}
			} else {
				fmt.Printf("Cannot parse %s\n", text)
			}
		}
		close(preload)
	}()
}

func MakeRecordReader(name string, params map[string]string) {
	// TODO: registry
	// noway to instance directly by type name in Golang
	switch name {
	case "file":
		return NewFileRecordReader(params["filename"])
	case "console":
		return NewConsoleRecordReader()
	default:
		return NewConsoleRecordReader()

	}
}
