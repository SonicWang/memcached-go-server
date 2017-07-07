package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"time"
	//	"github.com/pkg/profile" //uncomment to enable
	"sync/atomic"
)

/*
Constants for server connection.
*/
const (
	ConnHost = "localhost"
	ConnPort = "3333"
	ConnType = "tcp"
)

// RequestHeader is for representing a request header structure in Binary protocol.
type RequestHeader struct {
	Magic           uint8
	Opcode          uint8
	KeyLength       uint16
	ExtraLength     uint8
	DataType        uint8
	VBucketID       uint16
	TotalBodyLength uint32
	Opaque          uint32
	CAS             uint64
}

// ResponseHeader is for representing a response header structure in Binary protocol.
type ResponseHeader struct {
	Magic           uint8
	Opcode          uint8
	KeyLength       uint16
	ExtraLength     uint8
	DataType        uint8
	Status          uint16
	TotalBodyLength uint32
	Opaque          uint32
	CAS             uint64
}

// ConnectionContext is used as a context object during the life time of a connection.
// It contains re-usable buffer across commands, keeps track of connection information, and provided access to read/write network channel.
type ConnectionContext struct {
	RW          *bufio.ReadWriter
	ConnHandle  net.Conn
	ConnID      uint64 // Internal debug purpose
	StartTime   time.Time
	LastReqTime time.Time // For measuring how long a connection has been idle.
	CommandSeq  uint64    // Every connection starts counting command from 0
	ReadBuf     []byte    // Local to the goroutine handling a connection. Better utilizing memory.
}

/*
0x0000	No error
0x0001	Key not found
0x0002	Key exists
0x0003	Value too large
0x0004	Invalid arguments
0x0005	Item not stored
0x0006	Incr/Decr on non-numeric value.
0x0007	The vbucket belongs to another server
0x0008	Authentication error
0x0009	Authentication continue
0x0081	Unknown command
0x0082	Out of memory
0x0083	Not supported
0x0084	Internal error
0x0085	Busy
0x0086	Temporary failure
*/
const (
	CodeNoError     = 0x0000
	CodeKeyNotFound = 0x0001
	CodeKeyExists   = 0X0002
)

/*
0x00	Get
0x01	Set
0x02	Add
0x03	Replace
0x04	Delete
0x05	Increment
0x06	Decrement
0x07	Quit
0x08	Flush
0x09	GetQ
0x0a	No-op
0x0b	Version
0x0c	GetK
0x0d	GetKQ
0x0e	Append
0x0f	Prepend
0x10	Stat
0x11	SetQ
0x12	AddQ
0x13	ReplaceQ
0x14	DeleteQ
0x15	IncrementQ
0x16	DecrementQ
0x17	QuitQ
0x18	FlushQ
0x19	AppendQ
0x1a	PrependQ
0x1b	Verbosity *
0x1c	Touch *
0x1d	GAT *
0x1e	GATQ *
0x20	SASL list mechs
0x21	SASL Auth
0x22	SASL Step
0x30	RGet
0x31	RSet
0x32	RSetQ
0x33	RAppend
0x34	RAppendQ
0x35	RPrepend
0x36	RPrependQ
0x37	RDelete
0x38	RDeleteQ
0x39	RIncr
0x3a	RIncrQ
0x3b	RDecr
0x3c	RDecrQ
0x3d	Set VBucket *
0x3e	Get VBucket *
0x3f	Del VBucket *
0x40	TAP Connect *
0x41	TAP Mutation *
0x42	TAP Delete *
0x43	TAP Flush *
0x44	TAP Opaque *
0x45	TAP VBucket Set *
0x46	TAP Checkpoint Start *
0x47	TAP Checkpoint End *
*/
const (
	OpGet      = 0x00
	OpSet      = 0x01
	OpAdd      = 0x02
	OpReplace  = 0x03
	OpQuit     = 0x07
	OpGetQ     = 0x09
	OpNoOp     = 0x0a
	OpVersion  = 0x0b
	OpGetK     = 0x0c
	OpGetKQ    = 0x0d
	OpSetQ     = 0x11
	OpAddQ     = 0x12
	OpReplaceQ = 0x13
)

/*
Magic
0x80 Request
0x81 Response
*/
const (
	MagicRequest  = 0x80
	MagicResponse = 0x81
)

// MaxReqLen is the max body length of a request.
const MaxReqLen = 1024 * 1024 * 1024 // 1MB max request size

var connSeq uint64
var casID uint64

/*
   Byte/     0       |       1       |       2       |       3       |
      /              |               |               |               |
     |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
     +---------------+---------------+---------------+---------------+
    0| Magic         | Opcode        | Key length                    |
     +---------------+---------------+---------------+---------------+
    4| Extras length | Data type     | vbucket id                    |
     +---------------+---------------+---------------+---------------+
    8| Total body length                                             |
     +---------------+---------------+---------------+---------------+
   12| Opaque                                                        |
     +---------------+---------------+---------------+---------------+
   16| CAS                                                           |
     |                                                               |
     +---------------+---------------+---------------+---------------+
     Total 24 bytes
*/
func parseRequestHeader(bufHeader []byte) (RequestHeader, error) {
	ret := RequestHeader{}
	buf := bufHeader

	ret.Magic = uint8(buf[0])
	if ret.Magic != MagicRequest {
		return RequestHeader{}, fmt.Errorf("Magic byte is not 0x80: %x", ret.Magic)
	}
	buf = buf[1:]

	ret.Opcode = uint8(buf[0])
	_, ok := OpHandler[ret.Opcode]
	if !ok {
		return RequestHeader{}, fmt.Errorf("Opcode byte is not recognized: %x", ret.Opcode)
	}
	buf = buf[1:]

	ret.KeyLength = GetUint16(buf)
	buf = buf[2:]

	ret.ExtraLength = uint8(buf[0])
	buf = buf[1:]

	ret.DataType = uint8(buf[0])
	if ret.DataType != 0x00 {
		return RequestHeader{}, fmt.Errorf("DataType byte is supposed to be 0x00: %x", ret.DataType)
	}
	buf = buf[1:]

	ret.VBucketID = GetUint16(buf)
	buf = buf[2:]

	ret.TotalBodyLength = GetUint32(buf)
	if uint64(ret.TotalBodyLength) < uint64(ret.KeyLength)+uint64(ret.ExtraLength) {
		return RequestHeader{}, fmt.Errorf("TotaoBodyLength is supposed to be no less than KeyLength + ExtraLength: total: %d key: %d extra %d", ret.TotalBodyLength, ret.KeyLength, ret.ExtraLength)
	}
	buf = buf[4:]

	ret.Opaque = GetUint32(buf)
	buf = buf[4:]

	ret.CAS = GetUint64(buf)

	return ret, nil
}

/*
   Byte/     0       |       1       |       2       |       3       |
      /              |               |               |               |
     |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
     +---------------+---------------+---------------+---------------+
    0| Magic         | Opcode        | Key Length                    |
     +---------------+---------------+---------------+---------------+
    4| Extras length | Data type     | Status                        |
     +---------------+---------------+---------------+---------------+
    8| Total body length                                             |
     +---------------+---------------+---------------+---------------+
   12| Opaque                                                        |
     +---------------+---------------+---------------+---------------+
   16| CAS                                                           |
     |                                                               |
     +---------------+---------------+---------------+---------------+
     Total 24 bytes
*/
func writeResponseHeader(header ResponseHeader, rw *bufio.ReadWriter) error {
	err := rw.WriteByte(header.Magic)
	if err != nil {
		return err
	}

	err = rw.WriteByte(header.Opcode)
	if err != nil {
		return err
	}

	err = rw.WriteByte(GetNthByteFromUint16(header.KeyLength, 0))
	if err != nil {
		return err
	}
	err = rw.WriteByte(GetNthByteFromUint16(header.KeyLength, 1))
	if err != nil {
		return err
	}

	err = rw.WriteByte(header.ExtraLength)
	if err != nil {
		return err
	}

	err = rw.WriteByte(header.DataType)
	if err != nil {
		return err
	}

	err = rw.WriteByte(GetNthByteFromUint16(header.Status, 0))
	if err != nil {
		return err
	}
	err = rw.WriteByte(GetNthByteFromUint16(header.Status, 1))
	if err != nil {
		return err
	}

	for pos := 0; pos < 4; pos++ {
		err = rw.WriteByte(GetNthByteFromUint32(header.TotalBodyLength, pos))
		if err != nil {
			return err
		}
	}

	for pos := 0; pos < 4; pos++ {
		err = rw.WriteByte(GetNthByteFromUint32(header.Opaque, pos))
		if err != nil {
			return err
		}
	}

	l := uint32(header.CAS >> 32)
	r := uint32(header.CAS & 0x00000000ffffffff)
	for pos := 0; pos < 4; pos++ {
		err = rw.WriteByte(GetNthByteFromUint32(l, pos))
		if err != nil {
			return err
		}
	}
	for pos := 0; pos < 4; pos++ {
		err = rw.WriteByte(GetNthByteFromUint32(r, pos))
		if err != nil {
			return err
		}
	}
	return nil
}

func handleCommand(context *ConnectionContext) error {
	// Make a buffer to hold incoming data.
	bufHeader := context.ReadBuf[:24]
	readLen := 0
	for readLen < len(bufHeader) {
		reqLen, err := context.RW.Read(bufHeader[readLen:])
		if err != nil {
			return err
		}
		readLen += reqLen
	}
	context.CommandSeq++
	context.LastReqTime = time.Now()
	// fmt.Printf("Request header: %v\n", bufHeader)
	reqHeader, err := parseRequestHeader(bufHeader)
	if err != nil {
		fmt.Printf("Error parsing header: %s | % 20x\n", err, bufHeader)
		fmt.Fprintf(context.RW, "Error %s\n", err)
		return err
	}

	err = OpHandler[reqHeader.Opcode].Handle(reqHeader, context)
	return err
}

// Handles incoming requests.
func handleRequest(conn net.Conn) {
	defer conn.Close()
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	context := &ConnectionContext{
		ConnID:      atomic.AddUint64(&connSeq, 1),
		ConnHandle:  conn,
		StartTime:   time.Now(),
		LastReqTime: time.Now(),
		CommandSeq:  0,
		RW:          rw,
		ReadBuf:     make([]byte, 4096), // 4KB initial read buffer
	}
	defer rw.Flush()
	for {
		err := handleCommand(context)
		switch err {
		case nil:
			break
		case io.EOF:
			fmt.Printf("Client %s closed connection %d: connected at %s, handled %d commands.\n",
				context.ConnHandle.RemoteAddr().String(), context.ConnID, context.StartTime.String(), context.CommandSeq)
			return
		default:
			fmt.Println("Error reading:", err.Error())
			return
		}
		// force sending down a response
		rw.Flush()
	}
}

// Start starts the memcache server listening on TCP with Binary protocol support
func Start() {
	//	defer profile.Start().Stop() // uncomment to enable profiler
	// Listen for incoming connections.
	l, err := net.Listen(ConnType, ConnHost+":"+ConnPort)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	// Close the listener when the application closes.
	defer l.Close()
	fmt.Println("Listening on " + ConnHost + ":" + ConnPort)
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		// Handle connections in a new goroutine.
		go handleRequest(conn)
	}
}
