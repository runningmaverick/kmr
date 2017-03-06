package records

import "bufio"

type RecordWriter interface {
	WriteRecords([]Record) error
}

type SimpleRecordWriter struct {
	ioctx bufio.Writer
}

func (srw *SimpleRecordWriter) WriteRecords(records []Record) error {
	for r := range records {
		srw.ioctx.WriteString(fmt.Sprintf("%s %s\n", r.Key, r.Val))
	}
}

func NewConsoleRecordWriter() *SimpleRecordWriter {
	writer := bufio.NewWriter(os.Stdout)

	return &SimpleRecordReader{
		ioctx: writer,
	}
}

func NewFileRecordWriter(filename string) *SimpleRecordWriter {
	// TODO:
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		panic("fail to create file reader")
	}
	writer := bufio.NewWriter(file)

	return &SimpleRecordReader{
		ioctx: writer,
	}
}

func MakeRecordWriter(name string, params map[string]string) {
	// TODO: registry
	// noway to instance directly by type name in Golang
	switch name {
	case "file":
		return NewFileRecordWriter(params["filename"])
	case "console":
		return NewConsoleRecordWriter()
	default:
		return NewConsoleRecordWriter()

	}
}
