package bitcesque

func uint32ToBytes(targ []byte, idx uint64, val uint32) {
	targ[idx] = byte(val & 0xff)
	targ[idx+1] = byte((val >> 8) & 0xff)
	targ[idx+2] = byte((val >> 16) & 0xff)
	targ[idx+3] = byte((val >> 24) & 0xff)
}

func uint32FromBytes(targ []byte, idx uint64) uint32 {
	return uint32(targ[idx]) | uint32(targ[idx+1])<<8 | uint32(targ[idx+2])<<16 | uint32(targ[idx+3])<<24
}

func uint64ToBytes(targ []byte, idx uint64, val uint64) {
	targ[idx] = byte(val & 0xff)
	targ[idx+1] = byte((val >> 8) & 0xff)
	targ[idx+2] = byte((val >> 16) & 0xff)
	targ[idx+3] = byte((val >> 24) & 0xff)

	targ[idx+4] = byte((val >> 32) & 0xff)
	targ[idx+5] = byte((val >> 40) & 0xff)
	targ[idx+6] = byte((val >> 48) & 0xff)
	targ[idx+7] = byte((val >> 56) & 0xff)
}

func uint64FromBytes(targ []byte, idx uint64) uint64 {
	out := uint64(targ[idx]) | uint64(targ[idx+1])<<8 | uint64(targ[idx+2])<<16 | uint64(targ[idx+3])<<24
	out |= uint64(targ[idx+4])<<32 | uint64(targ[idx+5])<<40 | uint64(targ[idx+6])<<48 | uint64(targ[idx+7])<<56
	return out
}
