package bitcesque

import (
	"hash/crc32"
	"io/ioutil"
	"os"
	"syscall"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// This points into the document, directly at the value field
type offsetAndLength struct {
	offset uint64
	length uint32
}

// Generates the byte representation of the document, including the header.
// Empty v interpreted as tombstone.
func newDocument(k, v []byte) []byte {
	outsize := len(k) + len(v) + 12
	out := make([]byte, 12, outsize)
	uint32ToBytes(out, 4, uint32(len(k)))
	uint32ToBytes(out, 8, uint32(len(v)))
	out = append(out, k...)
	out = append(out, v...)
	hash := crc32.Checksum(out[4:], crcTable)
	uint32ToBytes(out, 0, hash)
	return out
}

// Return the appropriate value offset-and-length for the document, were it
// inserted at the given position.
func getOAL(pos uint64, k, v []byte) offsetAndLength {
	return offsetAndLength{pos + 12 + uint64(len(k)), uint32(len(v))}
}

// Returns the value in the DB at the given offset and length.
func (d *DB) getValAtOAL(oal offsetAndLength) []byte {
	return d.filebuffer[oal.offset : oal.offset+uint64(oal.length)]
}

// Takes a slice pointing at the entire document, including checksum, and
// verifies the checksum matches the contents.
func checkDocument(b []byte) bool {
	checksum := uint32FromBytes(b, 0)
	return checksum == crc32.Checksum(b[4:], crcTable)
}

// Rewrites backing file to contain only valid entries.
func (d *DB) Consolidate() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	tmp, e := ioutil.TempFile("", "")
	if e != nil {
		return e
	}
	mNew := make(map[string]offsetAndLength)
	pos := uint64(0)
	for k, oal := range d.kToPos {
		v := d.getValAtOAL(oal)
		doc := newDocument([]byte(k), v)
		newOAL := getOAL(pos, []byte(k), v)
		mNew[string(k)] = newOAL
		tmp.Write(doc)
		pos += uint64(len(doc))
	}
	e = d.filehandle.Close()
	if e != nil {
		return e
	}
	e = syscall.Munmap(d.filebuffer)
	if e != nil {
		return e
	}
	e = tmp.Close()
	if e != nil {
		return e
	}
	//Move new file to old loc
	e = os.Rename(tmp.Name(), d.location)
	if e != nil {
		return e
	}
	filehandle, e := os.OpenFile(d.location, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if e != nil {
		return e
	}
	buf, e := makeFilebuf(filehandle)
	if e != nil {
		return e
	}
	d.kToPos = mNew
	d.filehandle = filehandle
	d.filledSize = pos
	d.filebuffer = buf
	return nil
}

// Removes the given key from the DB, recording it as deleted.
func (d *DB) Remove(k []byte) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	doc := newDocument(k, []byte{})
	delete(d.kToPos, string(k))
	d.filehandle.Write(doc)
}

// Inserts or updates the given key with the given value.
func (d *DB) Upsert(k, v []byte) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	doc := newDocument(k, v)
	d.kToPos[string(k)] = getOAL(d.filledSize, k, v)
	d.filehandle.Write(doc)
	d.filledSize += uint64(len(doc))
	if d.filledSize > uint64(len(d.filebuffer)) {
		newLen := len(d.filebuffer) * 2
		syscall.Munmap(d.filebuffer)
		mmap, _ := syscall.Mmap(int(d.filehandle.Fd()), 0, newLen, syscall.PROT_READ, syscall.MAP_SHARED)
		d.filebuffer = mmap
	}
}

// Returns the value associated with the given key, and whether it is present.
func (d *DB) Get(k []byte) (string, bool) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	oal, present := d.kToPos[string(k)]
	if !present {
		return "", false
	}
	out := d.getValAtOAL(oal)
	return string(out), true
}

// Returns whether the given key exists in the DB.  Does not need to hit disk
// (unless you're under such memory pressure that you're swapping the keyfile
// as well).
func (d *DB) Contains(k []byte) bool {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	_, present := d.kToPos[string(k)]
	return present
}
