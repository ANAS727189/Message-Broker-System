package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync/atomic"

	"main/internal/protocol"
)

var correlationIDCounter uint32 = 0

func getNextCorrelationID() uint32 {
	return atomic.AddUint32(&correlationIDCounter, 1)
}

func main() {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	groupID := "worker-group-1"
	clientID := "consumer-client-1"

	for i := 0; i < 5; i++ {
		fmt.Printf("--- Fetching next message for group %s ---\n", groupID)

		// Create FETCH request header
		// Payload: [groupIDLen:4][groupID:...]
		groupIDBytes := []byte(groupID)
		payload := make([]byte, 4+len(groupIDBytes))
		binary.BigEndian.PutUint32(payload[0:4], uint32(len(groupIDBytes)))
		copy(payload[4:], groupIDBytes)

		hdr := &protocol.RequestHeader{
			Version:       protocol.ProtocolVersion,
			CorrelationID: getNextCorrelationID(),
			ClientID:      clientID,
			Command:       protocol.CommandFetch,
			Length:        uint32(len(payload)),
		}

		// Send FETCH request
		protocol.WriteRequestHeader(conn, hdr)
		conn.Write(payload)

		// Read response header
		respHdr, err := protocol.ReadResponseHeader(conn)
		if err != nil {
			fmt.Println("Error reading response header:", err)
			return
		}

		if respHdr.Status != 0 {
			// No message available
			fmt.Println("No message available yet")

			// Still need to read the offset from response
			offsetBuf := make([]byte, 8)
			io.ReadFull(conn, offsetBuf)
			offset := binary.BigEndian.Uint64(offsetBuf)
			fmt.Printf("Next offset to wait for: %d\n", offset)
			continue
		}

		// Read message from response
		// Response payload: [offset:8][msgLen:4][payload]
		respPayload := make([]byte, respHdr.Length)
		_, err = io.ReadFull(conn, respPayload)
		if err != nil {
			fmt.Println("Error reading response payload:", err)
			return
		}

		offset := binary.BigEndian.Uint64(respPayload[0:8])
		msgLen := binary.BigEndian.Uint32(respPayload[8:12])
		message := string(respPayload[12 : 12+msgLen])

		fmt.Printf("Received: %s (offset: %d)\n", message, offset)

		// Commit the offset
		if err := commitOffset(conn, groupID, clientID, offset); err != nil {
			fmt.Println("Error committing offset:", err)
			return
		}
	}
}

func commitOffset(conn net.Conn, groupID string, clientID string, offset uint64) error {
	groupIDBytes := []byte(groupID)
	payload := make([]byte, 4+len(groupIDBytes)+8)

	binary.BigEndian.PutUint32(payload[0:4], uint32(len(groupIDBytes)))
	copy(payload[4:], groupIDBytes)
	binary.BigEndian.PutUint64(payload[4+len(groupIDBytes):], offset)

	hdr := &protocol.RequestHeader{
		Version:       protocol.ProtocolVersion,
		CorrelationID: getNextCorrelationID(),
		ClientID:      clientID,
		Command:       protocol.CommandCommit,
		Length:        uint32(len(payload)),
	}

	protocol.WriteRequestHeader(conn, hdr)
	conn.Write(payload)

	// Read commit response
	respHdr, err := protocol.ReadResponseHeader(conn)
	if err != nil {
		return err
	}

	if respHdr.Status != 0 {
		return fmt.Errorf("commit failed")
	}

	fmt.Printf("Successfully committed offset %d for group %s\n", offset, groupID)
	return nil
}
