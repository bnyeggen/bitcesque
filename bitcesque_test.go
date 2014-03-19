package bitcesque

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestBitcesque(t *testing.T) {
	f, _ := ioutil.TempFile("", "bitcesque")
	t.Log("Test DB at: " + f.Name())
	f.Close()
	loc := f.Name()

	d, e := NewDB(loc)
	if e != nil {
		t.Error(e)
	}
	k1 := []byte("Tom")
	k2 := []byte("Dick")
	k3 := []byte("Harry")
	v1 := []byte("Washington")
	v2 := []byte("Oregon")
	v3 := []byte("Wisconsin")
	v4 := []byte("New York")
	v5 := []byte("Florida")

	d.Upsert(k1, v1)
	d.Upsert(k2, v2)
	d.Upsert(k3, v3)

	r1, _ := d.Get(k1)
	r2, _ := d.Get(k2)
	r3, _ := d.Get(k3)

	e = d.Sync()
	if e != nil {
		t.Error("Sync error")
	}

	if r1 != string(v1) || r2 != string(v2) || r3 != string(v3) {
		t.Error("Retrieval error")
	}

	d.Upsert(k1, v4)
	d.Upsert(k2, v5)
	d.Remove(k3)

	r1, _ = d.Get(k1)
	r2, _ = d.Get(k2)
	_, present := d.Get(k3)

	if r1 != string(v4) || r2 != string(v5) {
		t.Error("Update error")
	}
	if present {
		t.Error("Removal error")
	}

	e = d.Consolidate()
	r1, _ = d.Get(k1)
	r2, _ = d.Get(k2)
	_, present = d.Get(k3)

	if r1 != string(v4) || r2 != string(v5) || present {
		t.Error("Consolidation error")
	}

	d.Upsert(k1, v1)
	r1, _ = d.Get(k1)
	if r1 != string(v1) {
		t.Error("Upsert after consolidation error")
	}

	e = d.Close()
	if e != nil {
		t.Error("Close error")
	}

	d, e = OpenDB(loc)
	r1, _ = d.Get(k1)
	r2, _ = d.Get(k2)

	if r1 != string(v1) || r2 != string(v5) {
		t.Error("Error opening existing DB")
	}

	d.Upsert(k1, v2)
	r1, _ = d.Get(k1)
	if r1 != string(v2) {
		t.Error("Upsert after reopen error")
	}
	d.Close()

	d, e = OpenAndVerifyDB(loc)
	r1, _ = d.Get(k1)
	r2, _ = d.Get(k2)
	if r1 != string(v2) || r2 != string(v5) {
		t.Error("Error opening & verifying existing DB")
	}
	e = d.filehandle.Truncate(int64(d.filledSize) - 1)
	if e != nil {
		t.Error(e)
	}
	d.Close()

	d, e = OpenAndVerifyDB(loc)
	if e == nil {
		t.Error("Error verifying integrity")
	}
	d.Close()
	os.Remove(loc)
}
