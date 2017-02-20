package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {
	host := os.Args[0]
	port := os.Args[1]
	conn, err := net.Dial(host, string(port))
	if err != nil {
		fmt.Println("Connect failed.")
		os.Exit(0)
	}

	reader := bufio.NewReader(os.Stdin)

	for {
		sql, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Fail to read sql.")
		}

		if _, err := conn.Write([]byte(sql)); err != nil {
			fmt.Println("Fail to send sql.")
		}

		var buf []byte
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Fail to read result.")
		}
		fmt.Println(string(buf[:n]))
	}
}
