// Bitcesque implements a simple key / value store of arbitrary bytes, based
// essentially on Riak's Bitcask engine, but simplified for embedded usage.
package bitcesque

import (
	"errors"
	"os"
	"strconv"
	"sync"
	"syscall"
)

// Represents a collection of key / value pairs of arbitrary bytes.
type DB struct {
	kToPos     map[string]offsetAndLength
	location   string   //Location of underlying file
	filledSize uint64   //Writes happen at this position
	filehandle *os.File //Open file
	filebuffer []byte   //Mmap'd buffer over file, used only for reads
	mutex      sync.RWMutex
}

// Returns the location of the file backing the given DB.
func (d *DB) GetLocation() string {
	return d.location
}

func makeFilebuf(f *os.File) ([]byte, error) {
	stats, e := f.Stat()
	if e != nil {
		return nil, e
	}
	fLen := stats.Size()
	mmapLen := int(fLen)
	//Map at least 4gb to avoid constantly remapping, but never read invalid part
	//since it will have no pointers in
	if mmapLen < 4000000000 {
		mmapLen = 4000000000
	} else {
		mmapLen *= 2
	}
	return syscall.Mmap(int(f.Fd()), 0, mmapLen, syscall.PROT_READ, syscall.MAP_SHARED)
}

// Creates a new DB at the given location, *deleting* the data there.
func NewDB(location string) (*DB, error) {
	filehandle, e := os.OpenFile(location, os.O_TRUNC|os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if e != nil {
		return nil, e
	}
	mmap, e := makeFilebuf(filehandle)
	if e != nil {
		return nil, e
	}
	return &DB{
		make(map[string]offsetAndLength),
		location,
		0,
		filehandle,
		mmap,
		sync.RWMutex{},
	}, nil
}

// Opens a pre-existing database, loading its keystore.  Assumes validity.
func OpenDB(location string) (*DB, error) {
	filehandle, e := os.OpenFile(location, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if e != nil {
		return nil, e
	}
	stat, e := filehandle.Stat()
	if e != nil {
		return nil, e
	}
	pos := uint64(stat.Size())
	mmap, e := makeFilebuf(filehandle)
	if e != nil {
		return nil, e
	}
	out := &DB{
		make(map[string]offsetAndLength),
		location,
		pos,
		filehandle,
		mmap,
		sync.RWMutex{},
	}
	e = out.populateKeys()
	if e != nil {
		return nil, e
	}
	return out, nil
}

// Loads the pre-existing db at the given location, verifying its records
// and re-deriving the keyfile.  Intended to be called after an unclean
// shutdown.  If invalid records are encountered, loading is stopped and the
// db is returned with records up to that point, along with an error.
func OpenAndVerifyDB(location string) (*DB, error) {
	filehandle, e := os.OpenFile(location, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if e != nil {
		return nil, e
	}
	fi, _ := filehandle.Stat()
	fLen := uint64(fi.Size())
	mmap, e := makeFilebuf(filehandle)
	if e != nil {
		return nil, e
	}
	m := make(map[string]offsetAndLength)
	pos := uint64(0)
	for pos < uint64(fLen) {
		kLen := uint32FromBytes(mmap, pos+4)
		vLen := uint32FromBytes(mmap, pos+8)
		k := mmap[pos+12 : pos+12+uint64(kLen)]
		if checkDocument(mmap[pos : pos+12+uint64(kLen)+uint64(vLen)]) {
			if vLen > 0 {
				m[string(k)] = offsetAndLength{pos + 12 + uint64(kLen), vLen}
			} else {
				delete(m, string(k))
			}
		} else {
			return &DB{
				m,
				location,
				pos,
				filehandle,
				mmap,
				sync.RWMutex{},
			}, errors.New("Corruption detected starting at position " + strconv.FormatUint(pos, 10))
		}
		pos += 12 + uint64(kLen) + uint64(vLen)
	}
	return &DB{
		m,
		location,
		pos,
		filehandle,
		mmap,
		sync.RWMutex{},
	}, nil
}

// Close the DB after flushing to disk.
func (d *DB) Close() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	e := d.dumpKeys()
	e = syscall.Munmap(d.filebuffer)
	if e != nil {
		return e
	}
	return d.filehandle.Close()
}

// Flushes all DB writes to disk.
func (d *DB) Sync() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return d.filehandle.Sync()
}

// Returns the number of records contained in the given DB.
func (d *DB) Size() int {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	return len(d.kToPos)
}

// Returns a slice containing all current keys.
func (d *DB) Keys() []string {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	out := make([]string, 0, len(d.kToPos))
	for k, _ := range d.kToPos {
		out = append(out, k)
	}
	return out
}

// Returns a slice containing all current vals.
func (d *DB) Vals() []string {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	out := make([]string, 0, len(d.kToPos))
	for _, oal := range d.kToPos {
		out = append(out, string(d.getValAtOAL(oal)))
	}
	return out
}

// Returns the implicit string -> string map as a Go map.
func (d *DB) Dump() map[string]string {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	out := make(map[string]string, len(d.kToPos))
	for k, oal := range d.kToPos {
		out[k] = string(d.getValAtOAL(oal))
	}
	return out
}

// Returns a slice containing all current key / val pairs.
func (d *DB) KeysAndVals() [][2]string {
	d.mutex.RLock()
	defer d.mutex.RUnlock()
	out := make([][2]string, len(d.kToPos))
	for k, oal := range d.kToPos {
		kv := [2]string{k, string(d.getValAtOAL(oal))}
		out = append(out, kv)
	}
	return out
}

// Asynchronously returns all presently valid keys through the given channel.
// Retains a read lock until all keys have been written, then closes the channel.
func (d *DB) KeyChan(c chan string) {
	go func() {
		d.mutex.RLock()
		for k, _ := range d.kToPos {
			c <- k
		}
		close(c)
		d.mutex.RUnlock()
	}()
}

// Asynchronously returns all presently valid vals through the given channel.
// Retains a read lock all values have been written, then closes the channel.
func (d *DB) ValChan(c chan string) {
	go func() {
		d.mutex.RLock()
		for _, oal := range d.kToPos {
			c <- string(d.getValAtOAL(oal))
		}
		close(c)
		d.mutex.RUnlock()
	}()
}

// Asynchronously returns all presently valid key/val pairs through the given
// channel.  Retains a read lock until all pairs have been written, then closes
// the channel.
func (d *DB) keyAndValChan(c chan [2]string) {
	go func() {
		d.mutex.RLock()
		for k, oal := range d.kToPos {
			c <- [2]string{k, string(d.getValAtOAL(oal))}
		}
		close(c)
		d.mutex.RUnlock()
	}()
}
