package records

type Record struct {
	Key string
	Val string
}

type ByKey []Record

func (r ByKey) Len() int {
	return len(r)
}

func (r ByKey) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r ByKey) Less(i, j int) bool {
	return r[i].Key < r[j].Key
}
