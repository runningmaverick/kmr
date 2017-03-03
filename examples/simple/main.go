package main

import (
	"github.com/ceph/go-ceph/rados"
	"github.com/naturali/kmr/job"
)

func main() {
	// 1. create task
	//	- object_keys
	//	- mapper_class
	// 	- reducer_class
	//  - mapper_count: m
	//  - reducer_count: n

	// 2. generate map jobs: in the form of k8s job
	//	- object_keys
	// 	- mapper_class
	// 	- mapper_count
	//	- output_place
	//
	// mapper logic:
	// 	a: generate (key, value)
	//  b: result goes to bucket: (i, j)
	// 	 	i = serial id of mapper
	// 		j = consist_hash_to(0, n-1, key)

	// 3. dispatch jobs & tracking tasks status
	// 	dispatch and wait all to finish

	// 4. partition and shuffle ???
	// 	for buckets (0, j) -> (m, j)
	//		merge_to_a_single_bucket (j)
	//		with values of identical key group together or locate together
	//				1. key1:[v1, v2, v3]
	//				2.  key1: v1
	//	     			key1: v2
	//					key1: v3

	// 3. gnerate reduce jobs:
	//	- input_place
	// 	- output_place
	//	- reducer_class
	// 	- reducer_count

	// 5. dispatch and wait all to finish

	// each mapper output:
	//

	// TODO:
	// 	- k8s
	//  - ceph

	testjob := job.NewKubeJob()
	testjob.Launch()
	// fake inits
}

func getCephContext() *rados.IOContext {
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
	return ioctx
}

func playCeph() {
	args := []string{
		"--mon-host", "1.1.1.1",
	}
	conn, _ := rados.NewConn()
	err := conn.ParseCmdLineArgs(args)
	err = conn.Connect()

	ioctx, err := conn.OpenIOContext("ni")

	buff := make([]byte, 4<<20)
	count, err := ioctx.Read("objectkey", buff, 0)
}
