package minidb

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestFile(t *testing.T) {
	fpath := filepath.Join(os.TempDir(), strconv.Itoa(int(time.Now().Unix()))+".minidb")
	f, err := Open(fpath)
	if err != nil {
		panic(err)
	}

	testFile(t, f, true)

	f.Close()
	f, err = Open(fpath)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	testFile(t, f, false)
}

func testFile(t *testing.T, f *File, initial bool) {
	desc := "initial"
	if !initial {
		desc = "not initial"
	}
	t.Run(desc+": set and delete", func(t *testing.T) {
		// Set
		key := []byte("mykey")
		value := []byte("myvalue")
		if initial {
			err := f.Set(key, value)
			if err != nil {
				panic(err)
			}
		}

		// Get
		gotValue, err := f.Get(key)
		if err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		if !bytes.Equal(gotValue, value) {
			t.Fatalf("got value %q instead of %q", gotValue, value)
		}

		// Delete
		if !initial {
			err = f.Delete(key)
			if err != nil {
				panic(err)
			}
			_, gotErr := f.Get(key)
			if !errors.Is(gotErr, ErrKeyNotFound) {
				t.Fatalf("got err %q instead of %q", err, ErrKeyNotFound)
			}
		}
	})

	t.Run(desc+": set multiple and walk", func(t *testing.T) {
		// Set multiple values
		pairs := map[string]string{
			"0": "0",
			"1": "1",
			"2": "2",
		}
		if initial {
			for k, v := range pairs {
				err := f.Set([]byte(k), []byte(v))
				if err != nil {
					panic(err)
				}
			}
		}

		// Walk and check lexicographical order)
		gotKeys := []string{}
		err := f.WalkPrefixKeyValue([]byte{}, func(key, value []byte) error {
			gotKeys = append(gotKeys, string(key))
			return nil
		})
		if err != nil {
			panic(err)
		}
		for i, gotKey := range gotKeys {
			if i == 0 {
				continue
			}
			gotPreviousKey := gotKeys[i-1]
			if bytes.Compare([]byte(gotPreviousKey), []byte(gotKey)) == 1 {
				t.Fatalf("got %q before %q", gotPreviousKey, gotKey)
			}
		}
	})
}
