package records

type ReaderHeap []RecordReader

func (rh ReaderHeap) Len() int {
	return len(rh)
}

func (rh ReaderHeap) Less(i, j int) bool {
	return rh[i].Peek().Key < rh[j].Peek().Key
}

func (rh ReaderHeap) Swap(i, j int) {
	rh[i], rh[j] = rh[j], rh[i]
}

func (rh *ReaderHeap) Push(x interface{}) {
	*rh = append(*rh, x.(RecordReader))
}

func (rh *ReaderHeap) Pop() RecordReader {
	old := *rh
	n := len(old)
	x := old[n-1]
	*rh = old[0 : n-1]
	return x
}

func MergeReaders(readers []RecordReader, writer RecordWriter) {
	c := &ReaderHeap{}

	for i := 0; i < len(readers); i++ {
		if readers[i].HasNext() {
			c.Push(readers[i])
		}
	}

	for c.Len() > 0 {
		reader := c.Pop()
		r, err := reader.ReadRecord(1)
		if err != nil {
			writer.WriteRecord(r)
		}
		if reader.HasNext() {
			c.Push(reader)
		}
	}
}
