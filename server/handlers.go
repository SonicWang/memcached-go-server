package server

import (
	"errors"
	"fmt"
	"io"
	"time"
)

// Handler is the interface for all command handling functions.
type Handler interface {
	Handle(RequestHeader, *ConnectionContext) error
}

// HandleFunc implements Handler interface so we all command handling functions can be accessed through Handler interface.
type HandleFunc func(RequestHeader, *ConnectionContext) error

// Handle function serves as a proxy to calling its owning function.
func (f HandleFunc) Handle(header RequestHeader, ctx *ConnectionContext) error {
	return f(header, ctx)
}

// GetHandler handles GET/GETQ/GETK/GETKQ commands
var GetHandler HandleFunc = func(header RequestHeader, ctx *ConnectionContext) error {
	if header.ExtraLength > 0 {
		return fmt.Errorf("Get must NOT have ExtraLength: %d", header.ExtraLength)
	}
	if header.KeyLength == 0 {
		return errors.New("Get must have NONE-ZERON KeyLength")
	}
	if header.TotalBodyLength != uint32(header.KeyLength)+uint32(header.ExtraLength) {
		return fmt.Errorf("Get must NOT have value: total: %d keylength %d extralength %d", header.TotalBodyLength, header.KeyLength, header.ExtraLength)
	}
	if header.TotalBodyLength > uint32(len(ctx.ReadBuf)) {
		if header.TotalBodyLength > MaxReqLen {
			return fmt.Errorf("request size %d is too large than %d", header.TotalBodyLength, MaxReqLen)
		}
		nsize := len(ctx.ReadBuf)
		for nsize < int(header.TotalBodyLength) {
			nsize *= 2
		}
		ctx.ReadBuf = make([]byte, nsize)
	}
	buf := ctx.ReadBuf[:header.TotalBodyLength]
	readLen := 0
	for readLen < int(header.TotalBodyLength) {
		reqLen, err := ctx.RW.Read(buf[readLen:])
		if err != nil {
			return err
		}
		readLen += reqLen
	}

	// k/v storage access
	val, ok := GetFromSimpleKV(string(buf))

	respHeader := ResponseHeader{}
	respHeader.Magic = MagicResponse
	respHeader.Opcode = header.Opcode
	respHeader.Opaque = header.Opaque
	if !ok {
		// Not found
		if header.Opcode == OpGetQ || header.Opcode == OpGetKQ {
			//Q commands don't send responses upon cache miss
			return nil
		}
		respHeader.Status = CodeKeyNotFound
		respHeader.TotalBodyLength = uint32(len("Not found"))
		err := writeResponseHeader(respHeader, ctx.RW)
		if err != nil {
			return err
		}
		for _, c := range []byte("Not found") {
			err = ctx.RW.WriteByte(c)
			if err != nil {
				return err
			}
		}
	} else {
		// found
		respHeader.Status = CodeNoError
		respHeader.ExtraLength = 0x04
		if header.Opcode == OpGetK || header.Opcode == OpGetKQ {
			respHeader.KeyLength = uint16(len(buf))
		}
		respHeader.TotalBodyLength = uint32(len(val.RawData)) + uint32(respHeader.ExtraLength) + uint32(respHeader.KeyLength)
		respHeader.CAS = val.CAS
		err := writeResponseHeader(respHeader, ctx.RW)
		if err != nil {
			return err
		}
		for pos := 0; pos < 4; pos++ {
			err = ctx.RW.WriteByte(GetNthByteFromUint32(val.Flag, pos))
			if err != nil {
				return err
			}
		}
		if respHeader.KeyLength > 0 {
			writeLen := 0
			l := len(buf)
			for writeLen < l {
				n, err := ctx.RW.Write(buf)
				if err != nil {
					return err
				}
				writeLen += n
				buf = buf[n:]
			}
		}
		buf := val.RawData
		writeLen := 0
		for writeLen < len(val.RawData) {
			n, err := ctx.RW.Write(buf)
			if err != nil {
				return err
			}
			writeLen += n
			buf = buf[n:]
		}
	}
	return nil
}

// SetHandler handles SET/SETQ/ADD/ADDQ/REPLACE/REPLACEQ commands
var SetHandler HandleFunc = func(header RequestHeader, ctx *ConnectionContext) error {
	if header.ExtraLength != 8 || header.KeyLength == 0 {
		return fmt.Errorf("Set/Add/Replace commands MUST have key and extra : keylength %d, extralength: %d, totalbodylength: %d",
			header.KeyLength, header.ExtraLength, header.TotalBodyLength)
	}
	if header.TotalBodyLength > uint32(len(ctx.ReadBuf)) {
		if header.TotalBodyLength > MaxReqLen {
			return fmt.Errorf("request size %d is too large than %d", header.TotalBodyLength, MaxReqLen)
		}
		nsize := len(ctx.ReadBuf)
		for nsize < int(header.TotalBodyLength) {
			nsize *= 2
		}
		ctx.ReadBuf = make([]byte, nsize)
	}
	buf := ctx.ReadBuf[:header.TotalBodyLength]
	readLen := 0
	for readLen < int(header.TotalBodyLength) {
		reqLen, err := ctx.RW.Read(buf[readLen:])
		if err != nil {
			return err
		}
		readLen += reqLen
	}
	newFlag := GetUint32(buf)
	ttl := int(GetUint32(buf[4:]))
	if ttl > 0 {
		ttl += time.Now().Second()
	}
	key := string(buf[8 : 8+header.KeyLength])
	buf = buf[8+header.KeyLength:]
	newBuf := make([]byte, len(buf))
	copy(newBuf, buf)
	newVal := SimpleValue{
		RawData: newBuf,
		Flag:    newFlag,
		CAS:     0,
		TTL:     ttl,
	}

	shouldFail := false
	responseCode := CodeNoError
	responseCAS := uint64(0)

	// k/v storage access
	if header.Opcode == OpAdd || header.Opcode == OpAddQ {
		newVal, ok := AddToSimpleKV(key, newVal)
		if !ok {
			shouldFail = true
			responseCode = CodeKeyExists
			goto output
		}
		responseCAS = newVal.CAS
	} else {
		newVal, notfound, ok := SetToSimpleKV(key, newVal, header.CAS, header.Opcode == OpReplace || header.Opcode == OpReplaceQ)
		if notfound {
			shouldFail = true
			responseCode = CodeKeyNotFound
			goto output
		}
		if !ok {
			shouldFail = true
			responseCode = CodeKeyExists
			goto output
		}
		responseCAS = newVal.CAS
	}
output:
	respHeader := ResponseHeader{}
	respHeader.Magic = MagicResponse
	respHeader.Opcode = header.Opcode
	respHeader.Opaque = header.Opaque
	respHeader.Status = CodeNoError
	respHeader.CAS = responseCAS
	var errStr string

	if shouldFail {
		respHeader.Status = uint16(responseCode)
		if responseCode == CodeKeyNotFound {
			respHeader.TotalBodyLength = uint32(len("Not found"))
			errStr = "Not found"
		} else if responseCode == CodeKeyExists {
			respHeader.TotalBodyLength = uint32(len("Data exists for key."))
			errStr = "Data exists for key."
		}
	} else {
		// Q commands don't have response unless there's a failure
		if header.Opcode == OpAddQ || header.Opcode == OpReplaceQ || header.Opcode == OpSetQ {
			return nil
		}
	}
	err := writeResponseHeader(respHeader, ctx.RW)
	if err != nil {
		return err
	}
	for _, c := range []byte(errStr) {
		err = ctx.RW.WriteByte(c)
		if err != nil {
			return err
		}
	}
	return nil
}

// VersionHandler handles VERSION command
var VersionHandler HandleFunc = func(header RequestHeader, ctx *ConnectionContext) error {
	if header.KeyLength > 0 || header.ExtraLength > 0 || header.TotalBodyLength > 0 {
		return fmt.Errorf("Version command should have NO key, extra or value: keylength %d, extralength: %d, totalbodylength: %d",
			header.KeyLength, header.ExtraLength, header.TotalBodyLength)
	}
	respHeader := ResponseHeader{}
	respHeader.Magic = MagicResponse
	respHeader.Opcode = header.Opcode
	respHeader.Opaque = header.Opaque
	respHeader.Status = CodeNoError
	respHeader.TotalBodyLength = uint32(len("1.4.24")) // We fake a valid version
	err := writeResponseHeader(respHeader, ctx.RW)
	if err != nil {
		return err
	}
	buf := []byte("1.4.24")
	writeLen := 0
	for writeLen < len(buf) {
		n, err := ctx.RW.Write(buf)
		if err != nil {
			return err
		}
		writeLen += n
		buf = buf[n:]
	}
	return nil
}

// NoOpHandler handles NOOP command
var NoOpHandler HandleFunc = func(header RequestHeader, ctx *ConnectionContext) error {
	if header.KeyLength > 0 || header.ExtraLength > 0 || header.TotalBodyLength > 0 {
		return fmt.Errorf("NoOp command should have NO key, extra or value: keylength %d, extralength: %d, totalbodylength: %d",
			header.KeyLength, header.ExtraLength, header.TotalBodyLength)
	}
	respHeader := ResponseHeader{}
	respHeader.Magic = MagicResponse
	respHeader.Opcode = header.Opcode
	respHeader.Opaque = header.Opaque
	respHeader.Status = CodeNoError
	err := writeResponseHeader(respHeader, ctx.RW)
	if err != nil {
		return err
	}
	return nil
}

// QuitHandler handles QUIT command
var QuitHandler HandleFunc = func(header RequestHeader, ctx *ConnectionContext) error {
	if header.KeyLength > 0 || header.ExtraLength > 0 || header.TotalBodyLength > 0 {
		return fmt.Errorf("NoOp command should have NO key, extra or value: keylength %d, extralength: %d, totalbodylength: %d",
			header.KeyLength, header.ExtraLength, header.TotalBodyLength)
	}
	respHeader := ResponseHeader{}
	respHeader.Magic = MagicResponse
	respHeader.Opcode = header.Opcode
	respHeader.Opaque = header.Opaque
	respHeader.Status = CodeNoError
	err := writeResponseHeader(respHeader, ctx.RW)
	if err != nil {
		return err
	}
	return io.EOF
}

// OpHandler if the map from op -> command handler
// (TODO) Add more commands such as delete / incr/decr
var OpHandler = map[uint8]Handler{

	OpSet:      SetHandler,
	OpSetQ:     SetHandler,
	OpAdd:      SetHandler,
	OpAddQ:     SetHandler,
	OpReplace:  SetHandler,
	OpReplaceQ: SetHandler,
	OpGet:      GetHandler,
	OpGetQ:     GetHandler,
	OpGetK:     GetHandler,
	OpGetKQ:    GetHandler,
	OpVersion:  VersionHandler,
	OpNoOp:     NoOpHandler,
	OpQuit:     QuitHandler,
}
