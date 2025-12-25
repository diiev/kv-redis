package main

import (
	"fmt"
	"log"
	"net"
	"os"
)

func main() {
	log.Println("Reading config file")
	readConf("./redis.conf")
	l, err := net.Listen("tcp", ":6379")

	if err != nil {
		log.Fatal("Cannot listen on :6379")
	}

	defer l.Close()
	log.Println("listening on :6379 port")

	conn, err := l.Accept()

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer conn.Close()
	log.Println("Connection accepted")
	for {
		v := Value{typ: ARRAY}
		v.readArray(conn)
		handler(conn, &v)

	}

}
