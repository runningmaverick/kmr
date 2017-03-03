package records

type RecordWriter interface {
	WriteRecord([]Record) ([]Record, error)
}
