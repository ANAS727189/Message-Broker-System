package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"main/internal/protocol"
)

var correlationIDCounter uint32 = 0

func getNextCorrelationID() uint32 {
	return atomic.AddUint32(&correlationIDCounter, 1)
}

func main() {
	// 1. Connect to the broker
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	messages := []string{
		"Hello, Mini-Kafka!",
		"Distributed systems are fun.",
		"This is message number three.",
	}

	clientID := "producer-client-1"

	for _, m := range messages {
		// Prepare the payload
		payload := []byte(m)

		// Create request header
		hdr := &protocol.RequestHeader{
			Version:       protocol.ProtocolVersion,
			CorrelationID: getNextCorrelationID(),
			ClientID:      clientID,
			Command:       protocol.CommandProduce,
			Length:        uint32(len(payload)),
		}

		// Send header and payload
		protocol.WriteRequestHeader(conn, hdr)
		conn.Write(payload)

		// Read response header
		respHdr, err := protocol.ReadResponseHeader(conn)
		if err != nil {
			fmt.Println("Error reading response header:", err)
			return
		}

		if respHdr.Status != 0 {
			fmt.Println("Error response from broker")
			return
		}

		// Read response payload (message ID)
		idBuf := make([]byte, 8)
		_, err = io.ReadFull(conn, idBuf)
		if err != nil {
			fmt.Println("Error reading message ID:", err)
			return
		}

		offset := binary.BigEndian.Uint64(idBuf)
		fmt.Printf("Sent: '%s' | Acknowledged at Offset: %d\n", m, offset)

		time.Sleep(1 * time.Second)
	}
}
