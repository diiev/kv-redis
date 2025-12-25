package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
)

type ValueType string

const (
	ARRAY  ValueType = "*"
	BULK   ValueType = "$"
	STRING ValueType = "+"
	ERROR  ValueType = "-"
	NULL   ValueType = ""
)

type Value struct {
	typ   ValueType
	bulk  string
	str   string
	err   string
	array []Value
}

func (v *Value) readArray(reader io.Reader) {
	buf := make([]byte, 4)
	reader.Read(buf)
	fmt.Println(string(buf))
	arrLen, err := strconv.Atoi(string(buf[1]))

	if err != nil {
		fmt.Println(err)
		return
	}
	for range arrLen {
		bulk := v.readBulk(reader)
		v.array = append(v.array, bulk)
	}
}

func (v *Value) readBulk(reader io.Reader) Value {
	buf := make([]byte, 4)
	reader.Read(buf)

	n, err := strconv.Atoi(string(buf[1]))
	if err != nil {
		fmt.Println(err)
		return Value{}
	}
	bulkBuf := make([]byte, n+2)
	reader.Read(bulkBuf)
	bulk := string(bulkBuf[:n])

	return Value{typ: BULK, bulk: bulk}
}

func main() {
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

type Handler func(*Value) *Value

var Handlres = map[string]Handler{
	"COMMAND": command,
	"GET":     get,
	"SET":     set,
}
var DB = map[string]string{}

func command(v *Value) *Value {
	return &Value{typ: STRING, str: "OK"}
}
func get(v *Value) *Value {
	args := v.array[1:]
	if len(args) != 1 {
		return &Value{typ: ERROR, err: "ERR invalid number of arguments for 'GET' "}
	}
	name := args[0].bulk
	val, ok := DB[name]
	if !ok {
		return &Value{typ: NULL}
	}
	return &Value{typ: BULK, bulk: val}

}

func set(v *Value) *Value {
	args := v.array[1:]
	if len(args) != 2 {
		return &Value{typ: ERROR, err: "ERR invalid number of arguments for 'SET' "}
	}
	key := args[0].bulk
	val := args[1].bulk

	DB[key] = val

	return &Value{typ: STRING, str: "OK"}
}

func handler(conn net.Conn, v *Value) {
	cmd := v.array[0].bulk
	handler, ok := Handlres[cmd]
	if !ok {
		fmt.Println("Invalid Command", cmd)
		return

	}

	reply := handler(v)
	w := NewWriter(conn)
	w.Write(reply)
}

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{
		writer: bufio.NewWriter(w),
	}

}

func (w *Writer) Write(v *Value) {
	var reply string
	switch v.typ {
	case STRING:
		reply = fmt.Sprintf("%s%s\r\n", v.typ, v.str)
	case BULK:
		reply = fmt.Sprintf("%s%d\r\n%s\r\n", v.typ, len(v.bulk), v.bulk)
	case ERROR:
		reply = fmt.Sprintf("%s%s", v.typ, v.err)
	case NULL:
		reply = "$-1\r\n"
	}

	w.writer.Write([]byte(reply))
	w.writer.(*bufio.Writer).Flush()
}
