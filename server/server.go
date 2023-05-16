package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"

	dbengine "github.com/Avash027/midDB/db_engine"
	"github.com/Avash027/midDB/logger"
	LsmTree "github.com/Avash027/midDB/lsm_tree"
)

type Server struct {
	Port     string
	Host     string
	DBEngine *dbengine.DBEngine
}

func (s *Server) Start() {
	logs := logger.GetLogger()
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%s", s.Host, s.Port))
	if err != nil {
		logs.Err(err).Msg("Error listening")
		return
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			logs.Err(err).Msg("Error accepting")
			continue
		}

		go handleConnection(conn, s.DBEngine.LsmTree)
	}
}

func handleConnection(conn net.Conn, ltree *LsmTree.LSMTree) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	writer := bufio.NewWriter(conn)

	for scanner.Scan() {
		text := scanner.Text()

		cmd := strings.Split(text, " ")

		if len(cmd) == 0 {
			writer.WriteString("Invalid command\n")
		}

		switch cmd[0] {
		case "PUT":
			if len(cmd) != 3 {
				writer.WriteString("Invalid command\n")
				continue
			}
			ltree.Put(cmd[1], cmd[2])
			writer.WriteString("OK\n")
		case "GET":
			val, exist := ltree.Get(cmd[1])
			if !exist {
				writer.WriteString("Data not found\n")
			} else {
				writer.WriteString(val + "\n")
			}
		case "DEL":
			ltree.Del(cmd[1])
			writer.WriteString("OK\n")
		default:
			writer.WriteString("Invalid command\n")
		}

		writer.Flush()
	}

}
