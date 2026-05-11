package main

import (
	"fmt"
	"net"

	"main/internal/server"
	"main/internal/store"
)

func main() {
	st, err := store.New("./data/log", "messages")
	if err != nil {
		panic(err)
	}
	defer st.Close()

	l, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(err)
	}
	defer l.Close()

	fmt.Println("Broker online at :8080")

	for {
		conn, err := l.Accept()
		if err != nil {
			continue
		}

		go server.HandleConn(conn, st)
	}
}
