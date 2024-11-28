package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"
)

const (
	datagramSize = 516
	blockSize    = datagramSize - 4 // 4 bytes from header
)

type OpCode uint16

const (
	OpRRQ OpCode = iota + 1
	_
	OpData
	OpAck
	OpErr
)

type ErrCode uint16

const (
	ErrUnknown ErrCode = iota
	ErrNotFound
	ErrAccessViolation
	ErrDiskFull
	ErrIllegalOp
	ErrUnknownID
	ErrFileExists
	ErrNoUser
)

type ReadRequest struct {
	Filename string
	Mode     string
}

func (r ReadRequest) MarshalBinary() ([]byte, error) {
	mode := "octet"

	if r.Mode != "" {
		mode = r.Mode
	}

	cap := 2 + len(r.Filename) + 1 + len(r.Mode) + 1
	buf := bytes.Buffer{}
	buf.Grow(cap)
	//first write the operation code

	err := binary.Write(&buf, binary.BigEndian, OpRRQ) //bin of those numbers
	if err != nil {
		return nil, fmt.Errorf("write: %w", err)
	}

	//write the filename
	if _, err := buf.WriteString(r.Filename); err != nil {
		return nil, fmt.Errorf("writeString: %w", err)
	}

	//0 byte
	if err := buf.WriteByte(0); err != nil {
		return nil, fmt.Errorf("writeByte: %w", err)
	}

	if _, err := buf.WriteString(mode); err != nil {
		return nil, fmt.Errorf("writeString: %w", err)
	}

	if err := buf.WriteByte(0); err != nil {
		return nil, fmt.Errorf("writeByte: %w", err)
	}

	return buf.Bytes(), nil
}

func (r *ReadRequest) UnmarshalBinary(bs []byte) error {
	//we create a buf from bs to work easier
	buf := bytes.NewBuffer(bs)

	var opCode OpCode
	if err := binary.Read(buf, binary.BigEndian, &opCode); err != nil {
		return err
	}

	if opCode != OpRRQ {
		return errors.New("invalid OpCode")
	}

	//read till this byte "0"
	var err error
	r.Filename, err = buf.ReadString(0)
	if err != nil {
		return err
	}

	//0 needs to be remove
	r.Filename = strings.TrimRight(r.Filename, "\x00") //

	if len(r.Filename) == 0 {
		return errors.New("invalid filename")
	}

	r.Mode, err = buf.ReadString(0)
	if err != nil {
		return errors.New("invalid mode")
	}

	r.Mode = strings.TrimRight(r.Mode, "\x00")

	actual := strings.ToLower(r.Mode)
	if actual != "octet" {
		return errors.New("only octet mode supported")
	}
	return nil
}

type Data struct {
	Block   uint16 // 2 bytes for block number
	Payload io.Reader
}

func (d *Data) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)

	//we have a max size for datagram
	buf.Grow(datagramSize)

	d.Block++
	if err := binary.Write(buf, binary.BigEndian, OpData); err != nil {
		return nil, err
	}

	//block number
	if err := binary.Write(buf, binary.BigEndian, d.Block); err != nil {
		return nil, err
	}

	//copy from the reader into "buf"
	_, err := io.CopyN(buf, d.Payload, blockSize)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (d *Data) UnmarshalBinary(bs []byte) error {
	length := len(bs)
	if length < 4 || length > datagramSize {
		return errors.New("invalid data")
	}

	buf := bytes.NewReader(bs)

	var opCode OpCode
	if err := binary.Read(buf, binary.BigEndian, &opCode); err != nil {
		return err
	}

	//read block size
	if err := binary.Read(buf, binary.BigEndian, &d.Block); err != nil {
		return err
	}

	//reset is payload
	d.Payload = buf

	return nil
}

type Ack uint16 //4 bytes

func (a Ack) MarshalBinary() ([]byte, error) {
	cap := 2 + 2
	b := new(bytes.Buffer)
	b.Grow(cap)

	if err := binary.Write(b, binary.BigEndian, OpAck); err != nil {
		return nil, err
	}

	if err := binary.Write(b, binary.BigEndian, a); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (a *Ack) UnmarshalBinary(bs []byte) error {
	var opCode OpCode
	r := bytes.NewReader(bs)

	if err := binary.Read(r, binary.BigEndian, &opCode); err != nil {
		return err
	}

	if opCode != OpAck {
		return errors.New("invalid opcode")
	}

	if err := binary.Read(r, binary.BigEndian, a); err != nil {
		return err
	}

	return nil
}

type Err struct {
	Code    ErrCode
	Message string
}

func (e Err) MarshalBinary() ([]byte, error) {
	cap := 2 + 2 + len(e.Message) + 1
	b := new(bytes.Buffer)
	b.Grow(cap)

	if err := binary.Write(b, binary.BigEndian, OpErr); err != nil {
		return nil, err
	}

	if err := binary.Write(b, binary.BigEndian, e.Code); err != nil {
		return nil, err
	}

	//message does not need to use binary pkg
	if _, err := b.WriteString(e.Message); err != nil {
		return nil, err
	}
	//write zero byte at end
	if err := b.WriteByte(0); err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func (e *Err) UnmarshalBinary(bs []byte) error {
	r := bytes.NewBuffer(bs)

	var opCode OpCode

	if err := binary.Read(r, binary.BigEndian, &opCode); err != nil {
		return err
	}

	if opCode != OpErr {
		return errors.New("invalid opt")
	}

	if err := binary.Read(r, binary.BigEndian, &e.Code); err != nil {
		return err
	}

	//message
	var err error
	e.Message, err = r.ReadString('\x00')
	if err != nil {
		return err
	}
	e.Message = strings.TrimRight(e.Message, "\x00")
	return nil
}

type Server struct {
	Payload []byte
	Retries uint8
	Timeout time.Duration
}

func (s *Server) ListenAndServe(addr string) error {
	conn, err := net.ListenPacket("udp", addr)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	log.Printf("listening on %s...\n", addr)
	return s.Serve(conn)
}

func (s *Server) Serve(conn net.PacketConn) error {
	if conn == nil {
		return errors.New("connection is nil")
	}

	if s.Payload == nil {
		return errors.New("payload is required")
	}

	if s.Retries == 0 {
		s.Retries = 10
	}

	if s.Timeout == 0 {
		s.Timeout = time.Second * 6
	}

	var request ReadRequest

	for {
		buf := make([]byte, datagramSize)
		_, addr, err := conn.ReadFrom(buf)
		if err != nil {
			return err
		}

		err = request.UnmarshalBinary(buf)
		if err != nil {
			log.Printf("[%s] bad request: %s", addr, err)
			return err
		}

		go s.Handle(addr.String(), request)
	}
}

func (s *Server) Handle(clientAddr string, readReq ReadRequest) {
	log.Printf("[%s] asking for file [%s]\n", clientAddr, readReq.Filename)

	//create a new udp conn to that addr
	conn, err := net.Dial("udp", clientAddr)
	if err != nil {
		log.Printf("dial: [%s], err: %s", clientAddr, err)
		return
	}

	defer conn.Close()

	var (
		ackPkt  Ack
		errPkt  Err
		dataPkt = Data{Payload: bytes.NewReader(s.Payload)}
		buf     = make([]byte, datagramSize)
	)

NextPacket:
	for n := datagramSize; n == datagramSize; {
		data, err := dataPkt.MarshalBinary()
		if err != nil {
			log.Printf("[%s]: preparing data: %s\n", clientAddr, err)
			return
		}

	Retry:
		for i := s.Retries; i > 0; i++ {
			//if the number of bytes written is 516 bytes we loop again otherwise we break
			n, err = conn.Write(data)
			if err != nil {
				log.Printf("[%s]: writing data: %s\n", clientAddr, err)
				return
			}

			//set a deadline on the conn as long we are waiting for client ack
			_ = conn.SetDeadline(time.Now().Add(s.Timeout))

			//we read from conn now this will block till either "timeout happen" or "io.EOF" or "Some"
			_, err = conn.Read(buf)
			if err != nil {
				//if the error is a timeout err we retry one more time
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue Retry
				} else {
					//some thing went wrong
					log.Printf("[%s] waiting for ACK: %v", clientAddr, err)
					return
				}
			}

			//now we parse client response
			switch {
			case ackPkt.UnmarshalBinary(buf) == nil:
				//if it can be unmarshal to act, then we have an ack

				//check if its the ack for the current block
				if uint16(ackPkt) == dataPkt.Block {
					//received ack send next piece
					continue NextPacket
				}

			case errPkt.UnmarshalBinary(buf) == nil:
				//log the err
				log.Printf("[%s] received error: %v\n", clientAddr, err)
				return
			default:
				//log something went wrong
				log.Printf("[%s] bad packet", clientAddr)
			}

		}

		//if we get here, the retries are done
		log.Printf("[%s] exhausted retries", clientAddr)
		return
	}

	log.Printf("[%s] sent %d blocks", clientAddr, dataPkt.Block)
}
