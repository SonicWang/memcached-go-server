package server

// GetUint16 converts first 2 bytes in a slice to uint16
func GetUint16(buf []byte) uint16 {
	return uint16(buf[0])<<8 | uint16(buf[1])
}

// GetUint32 converts first 4 bytes in a slice to uint32
func GetUint32(buf []byte) uint32 {
	return uint32(GetUint16(buf))<<16 | uint32(GetUint16(buf[2:]))
}

// GetUint64 converts first 8 bytes in a slice to uint64
func GetUint64(buf []byte) uint64 {
	return uint64(GetUint32(buf))<<32 | uint64(GetUint32(buf[4:]))
}

// GetNthByteFromUint16 gets the pos-th byte from an uint16. Ordering is from msb to lsb.
func GetNthByteFromUint16(v uint16, pos int) byte {
	switch pos {
	case 0:
		return byte(v >> 8)
	default:
		return byte(v & 0x00ff)
	}
}

// GetNthByteFromUint32 gets the pos-th byte from an uint32. Ordering is from msb to lsb.
func GetNthByteFromUint32(v uint32, pos int) byte {
	l := uint16(v >> 16)
	r := uint16(v & 0x0000ffff)
	switch pos {
	case 0:
		return GetNthByteFromUint16(l, 0)
	case 1:
		return GetNthByteFromUint16(l, 1)
	case 2:
		return GetNthByteFromUint16(r, 0)
	default:
		return GetNthByteFromUint16(r, 1)
	}
}
