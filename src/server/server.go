package main

import (
	"../sql/parser"
	"../sql/planner"
	"log"
	"net"
)

func main() {

	l, err := net.Listen("tcp", ":2000")
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	for {
		// Wait for a connection.
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		var buf []byte
		n, err := conn.Read(buf)
		if err != nil {
			conn.Close()
		}

		sql := string(buf[:n])
		result, err := Eval(sql)

		if err != nil {
			result = err.Error()
		}

		conn.Write([]byte(result))
		conn.Close()
	}

}

func Eval(sql string) (string, error) {
	appliable, err := parser.Parse(sql)
	if err != nil {
		return "Wrong SQL", parser.ParsedErr
	}
	return planner.Eval(appliable), nil
}
