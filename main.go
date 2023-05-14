package main

import (
	"bufio"
	"fmt"
	"net"

	"github.com/Avash027/midDB/logger"
)

func main() {
	logger.LoggerInit("LOCAL")

	listener, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		return
	}
	defer listener.Close()
	fmt.Println("Listening on localhost:8080")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	writer := bufio.NewWriter(conn)

	for scanner.Scan() {
		text := scanner.Text()
		fmt.Println("Received:", text)

		writer.Flush()
	}
}
