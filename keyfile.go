package bitcesque

import (
	"os"
	"syscall"
)

// Dumps current map from db to d.location + ".keys"
func (d *DB) dumpKeys() error {
	loc := d.location + ".keys"
	filehandle, e := os.OpenFile(loc, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if e != nil {
		return e
	}
	for k, v := range d.kToPos {
		buf := make([]byte, 16, 16+len(k))
		uint32ToBytes(buf, 0, uint32(len(k)))
		uint32ToBytes(buf, 4, v.length)
		uint64ToBytes(buf, 8, v.offset)
		buf = append(buf, k...)
		filehandle.Write(buf)
	}
	return filehandle.Close()
}

// Mutatively populates the keys of a partially initialized DB based on the
// keyfile in the appropriate location.  Meant to be called during
// initialization, so does not lock the db.
func (d *DB) populateKeys() error {
	loc := d.location + ".keys"
	filehandle, e := os.OpenFile(loc, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if e != nil {
		return e
	}
	stats, e := filehandle.Stat()
	if e != nil {
		return e
	}
	mmap, e := syscall.Mmap(int(filehandle.Fd()), 0, int(stats.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if e != nil {
		return e
	}
	e = syscall.Madvise(mmap, syscall.MADV_SEQUENTIAL)
	if e != nil {
		return e
	}
	m := make(map[string]offsetAndLength)
	pos := uint64(0)
	for pos < uint64(len(mmap)) {
		kLen := uint32FromBytes(mmap, pos)
		vLen := uint32FromBytes(mmap, pos+4)
		vPos := uint64FromBytes(mmap, pos+8)
		k := mmap[pos+16 : pos+16+uint64(kLen)]
		pos = pos + 16 + uint64(kLen)
		m[string(k)] = offsetAndLength{vPos, vLen}
	}
	e = syscall.Munmap(mmap)
	if e != nil {
		return e
	}
	d.kToPos = m
	return nil
}
