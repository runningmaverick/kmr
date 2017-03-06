package main

import (
	"fmt"
	"io/ioutil"

	"github.com/ceph/go-ceph/rados"
)

func main() {
	// for test ceph, ignore this file
	mons := "172.11.51.7:6789"
	secret := []byte("AQB+OJ9YAJSoHhAACOtovk+EY2QDiEksGM1XkA==")

	conn, _ := rados.NewConn()
	err := ioutil.WriteFile("ceph-keyfile", secret, 0600)
	if err != nil {
		fmt.Println("Failed to store keyfile:", err)
		return
	}

	args := []string{"--mon-host", mons, "--keyfile", "ceph-keyfile"}
	err = conn.ParseCmdLineArgs(args)
	if err != nil {
		fmt.Println("Failed to Parse args:", err)
		return
	}

	err = conn.Connect()
	if err != nil {
		fmt.Println("Failed to connect:", err)
		return
	}

	ioctx, err := conn.OpenIOContext("kmr")
	if err != nil {
		fmt.Println(fmt.Sprintf("Cannot open %s:", "kmr"), err)
		return
	}
	bytes_in := []byte("input data j")
	err = ioctx.Write("obj", bytes_in, 0)
	if err != nil {
		fmt.Println(fmt.Sprintf("Cannot write %s:", "obj"), err)
		return
	}

	bytes_out := make([]byte, 5)
	var offset uint64 = 0
	for {
		n, err := ioctx.Read("obj", bytes_out, offset)
		if err != nil {
			fmt.Printf("Cannot write %s, err: %v\n", "obj", err)
			return
		}
		if n > 0 {
			offset += uint64(n)
		} else {
			fmt.Printf("EOF")
			return
		}
		fmt.Printf("read count: %d, content: %s\n", n, bytes_out[:n])
	}

	fmt.Println("Finish")
}
