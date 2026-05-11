package protocol

import (
	"encoding/binary"
	"io"
)

const (
	// Protocol version
	ProtocolVersion uint32 = 1

	// Commands
	CommandProduce byte = 1
	CommandConsume byte = 2
	CommandCommit  byte = 3
	CommandFetch   byte = 4
)

// Request header: [version:4][correlationID:4][clientID:4][clientIDLen:4][command:1][length:4]
type RequestHeader struct {
	Version       uint32
	CorrelationID uint32
	ClientID      string
	Command       byte
	Length        uint32
}

// Response header: [version:4][correlationID:4][status:1][length:4]
type ResponseHeader struct {
	Version       uint32
	CorrelationID uint32
	Status        uint8 // 0 = success, 1 = error
	Length        uint32
}

func WriteRequestHeader(w io.Writer, hdr *RequestHeader) error {
	buf := make([]byte, 4+4+4+len(hdr.ClientID)+1+4)
	idx := 0

	binary.BigEndian.PutUint32(buf[idx:], hdr.Version)
	idx += 4

	binary.BigEndian.PutUint32(buf[idx:], hdr.CorrelationID)
	idx += 4

	binary.BigEndian.PutUint32(buf[idx:], uint32(len(hdr.ClientID)))
	idx += 4

	copy(buf[idx:], []byte(hdr.ClientID))
	idx += len(hdr.ClientID)

	buf[idx] = hdr.Command
	idx += 1

	binary.BigEndian.PutUint32(buf[idx:], hdr.Length)
	idx += 4

	_, err := w.Write(buf)
	return err
}

func ReadRequestHeader(r io.Reader) (*RequestHeader, error) {
	buf := make([]byte, 4+4+4)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	hdr := &RequestHeader{}
	hdr.Version = binary.BigEndian.Uint32(buf[0:4])
	hdr.CorrelationID = binary.BigEndian.Uint32(buf[4:8])
	clientIDLen := binary.BigEndian.Uint32(buf[8:12])

	clientIDBuf := make([]byte, clientIDLen+1+4)
	if _, err := io.ReadFull(r, clientIDBuf); err != nil {
		return nil, err
	}

	hdr.ClientID = string(clientIDBuf[:clientIDLen])
	hdr.Command = clientIDBuf[clientIDLen]
	hdr.Length = binary.BigEndian.Uint32(clientIDBuf[clientIDLen+1:])

	return hdr, nil
}

func WriteResponseHeader(w io.Writer, hdr *ResponseHeader) error {
	buf := make([]byte, 4+4+1+4)
	binary.BigEndian.PutUint32(buf[0:4], hdr.Version)
	binary.BigEndian.PutUint32(buf[4:8], hdr.CorrelationID)
	buf[8] = hdr.Status
	binary.BigEndian.PutUint32(buf[9:13], hdr.Length)

	_, err := w.Write(buf)
	return err
}

func ReadResponseHeader(r io.Reader) (*ResponseHeader, error) {
	buf := make([]byte, 4+4+1+4)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	hdr := &ResponseHeader{}
	hdr.Version = binary.BigEndian.Uint32(buf[0:4])
	hdr.CorrelationID = binary.BigEndian.Uint32(buf[4:8])
	hdr.Status = buf[8]
	hdr.Length = binary.BigEndian.Uint32(buf[9:13])

	return hdr, nil
}
