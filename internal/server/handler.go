package server

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"main/internal/protocol"
	"main/internal/store"
)

func HandleConn(conn net.Conn, st *store.Store) {
	defer conn.Close()
	for {
		hdr, err := protocol.ReadRequestHeader(conn)
		if err != nil {
			return
		}

		payload := make([]byte, hdr.Length)
		if hdr.Length > 0 {
			if _, err := io.ReadFull(conn, payload); err != nil {
				return
			}
		}

		switch hdr.Command {
		case protocol.CommandProduce:
			handleProduce(conn, st, hdr, payload)
		case protocol.CommandConsume:
			handleConsume(conn, st, hdr, payload)
		case protocol.CommandCommit:
			handleCommit(conn, st, hdr, payload)
		case protocol.CommandFetch:
			handleFetch(conn, st, hdr, payload)
		default:
			return
		}
	}
}

func handleProduce(conn net.Conn, st *store.Store, hdr *protocol.RequestHeader, payload []byte) {
	id, err := st.Append(payload)

	respHdr := &protocol.ResponseHeader{
		Version:       protocol.ProtocolVersion,
		CorrelationID: hdr.CorrelationID,
		Status:        0,
		Length:        8,
	}

	if err != nil {
		respHdr.Status = 1
		respHdr.Length = 0
	}

	protocol.WriteResponseHeader(conn, respHdr)

	if err == nil {
		idBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(idBuf, id)
		conn.Write(idBuf)
	}

	fmt.Printf("Logged: %s as Message ID %d\n", string(payload), id)
}

func handleConsume(conn net.Conn, st *store.Store, hdr *protocol.RequestHeader, payload []byte) {
	if len(payload) < 8 {
		respHdr := &protocol.ResponseHeader{
			Version:       protocol.ProtocolVersion,
			CorrelationID: hdr.CorrelationID,
			Status:        1,
			Length:        0,
		}
		protocol.WriteResponseHeader(conn, respHdr)
		return
	}

	requestedID := binary.BigEndian.Uint64(payload[0:8])
	data, err := st.ReadByID(requestedID)

	respHdr := &protocol.ResponseHeader{
		Version:       protocol.ProtocolVersion,
		CorrelationID: hdr.CorrelationID,
		Status:        0,
	}

	if err != nil {
		respHdr.Status = 1
		respHdr.Length = 0
		protocol.WriteResponseHeader(conn, respHdr)
		return
	}

	respHdr.Length = uint32(len(data))
	protocol.WriteResponseHeader(conn, respHdr)
	conn.Write(data)
}

func handleCommit(conn net.Conn, st *store.Store, hdr *protocol.RequestHeader, payload []byte) {
	if len(payload) < 4 {
		respHdr := &protocol.ResponseHeader{
			Version:       protocol.ProtocolVersion,
			CorrelationID: hdr.CorrelationID,
			Status:        1,
			Length:        0,
		}
		protocol.WriteResponseHeader(conn, respHdr)
		return
	}

	groupIDLen := binary.BigEndian.Uint32(payload[0:4])
	if uint32(len(payload)) < 4+groupIDLen+8 {
		respHdr := &protocol.ResponseHeader{
			Version:       protocol.ProtocolVersion,
			CorrelationID: hdr.CorrelationID,
			Status:        1,
			Length:        0,
		}
		protocol.WriteResponseHeader(conn, respHdr)
		return
	}

	groupID := string(payload[4 : 4+groupIDLen])
	offset := binary.BigEndian.Uint64(payload[4+groupIDLen : 4+groupIDLen+8])

	st.CommitOffset(groupID, offset)

	respHdr := &protocol.ResponseHeader{
		Version:       protocol.ProtocolVersion,
		CorrelationID: hdr.CorrelationID,
		Status:        0,
		Length:        0,
	}
	protocol.WriteResponseHeader(conn, respHdr)

	fmt.Printf("Group %s committed offset %d\n", groupID, offset)
}

func handleFetch(conn net.Conn, st *store.Store, hdr *protocol.RequestHeader, payload []byte) {
	if len(payload) < 4 {
		respHdr := &protocol.ResponseHeader{
			Version:       protocol.ProtocolVersion,
			CorrelationID: hdr.CorrelationID,
			Status:        1,
			Length:        0,
		}
		protocol.WriteResponseHeader(conn, respHdr)
		return
	}

	groupIDLen := binary.BigEndian.Uint32(payload[0:4])
	if uint32(len(payload)) < 4+groupIDLen {
		respHdr := &protocol.ResponseHeader{
			Version:       protocol.ProtocolVersion,
			CorrelationID: hdr.CorrelationID,
			Status:        1,
			Length:        0,
		}
		protocol.WriteResponseHeader(conn, respHdr)
		return
	}

	groupID := string(payload[4 : 4+groupIDLen])

	// Get the next offset for this consumer group
	nextOffset, err := st.GetNextOffset(groupID)
	if err != nil {
		respHdr := &protocol.ResponseHeader{
			Version:       protocol.ProtocolVersion,
			CorrelationID: hdr.CorrelationID,
			Status:        1,
			Length:        0,
		}
		protocol.WriteResponseHeader(conn, respHdr)
		return
	}

	// Try to read the message at nextOffset
	data, err := st.ReadByID(nextOffset)

	respHdr := &protocol.ResponseHeader{
		Version:       protocol.ProtocolVersion,
		CorrelationID: hdr.CorrelationID,
		Status:        0,
	}

	if err != nil {
		// No message available at this offset
		respHdr.Status = 1
		respHdr.Length = 8 // Just return the offset
		protocol.WriteResponseHeader(conn, respHdr)
		offsetBuf := make([]byte, 8)
		binary.BigEndian.PutUint64(offsetBuf, nextOffset)
		conn.Write(offsetBuf)
		return
	}

	// Message found: [offset:8][msgLen:4][payload]
	respPayload := make([]byte, 8+4+len(data))
	binary.BigEndian.PutUint64(respPayload[0:8], nextOffset)
	binary.BigEndian.PutUint32(respPayload[8:12], uint32(len(data)))
	copy(respPayload[12:], data)

	respHdr.Length = uint32(len(respPayload))
	protocol.WriteResponseHeader(conn, respHdr)
	conn.Write(respPayload)

	fmt.Printf("Fetched for group %s at offset %d\n", groupID, nextOffset)
}
