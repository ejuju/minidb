package minidb

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
)

type File struct {
	r, w    *os.File
	woffset int
	keymap  *keymap
}

func Open(fpath string) (*File, error) {
	f := &File{}

	var err error
	f.r, f.w, err = openFileRW(fpath)
	if err != nil {
		return nil, err
	}

	// Reconstruct in-memory database keymap
	f.keymap = &keymap{root: &keymapNode{}}
	bufr := bufio.NewReader(f.r)
	for {
		n, op, key, _, err := DecodeRowFromReader(bufr)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%w (at offset %d)", err, f.woffset)
		}
		switch op {
		default:
			return nil, fmt.Errorf("unknown op: %q", op)
		case OpSet:
			f.keymap.set(key, &byteRange{offset: f.woffset, length: n})
		case OpDelete:
			f.keymap.delete(key)
		}
		f.woffset += n
	}

	return f, nil
}

func openFileRW(fpath string) (*os.File, *os.File, error) {
	var r, w *os.File
	var err error
	if r, err = os.OpenFile(fpath, os.O_RDONLY|os.O_CREATE, 0666); err != nil {
		return nil, nil, err
	}
	if w, err = os.OpenFile(fpath, os.O_WRONLY|os.O_APPEND, 0666); err != nil {
		return nil, nil, err
	}
	return r, w, nil
}

func (f *File) Close() error {
	if err := f.r.Close(); err != nil {
		return err
	}
	if err := f.w.Close(); err != nil {
		return err
	}
	return nil
}

func (f *File) Set(key, value []byte) error {
	row, err := FormatRow(OpSet, key, value)
	if err != nil {
		return err
	}
	n, err := f.writeRow(row)
	if err != nil {
		return err
	}
	f.keymap.set(key, &byteRange{offset: f.woffset - n, length: n})
	return nil
}

func (f *File) Delete(key []byte) error {
	row, err := FormatRow(OpDelete, key, nil)
	if err != nil {
		return err
	}
	_, err = f.writeRow(row)
	if err != nil {
		return err
	}
	f.keymap.delete(key)
	return nil
}

func (f *File) writeRow(row []byte) (int, error) {
	n, err := f.w.Write(row)
	if err != nil {
		return n, err
	}
	f.woffset += n
	return n, nil
}

func (f *File) Count() int {
	n := 0
	_ = f.keymap.walk([]byte{}, func(_ []byte, _ *byteRange) error { n++; return nil })
	return n
}

func (f *File) Has(key []byte) bool { return f.keymap.get(key) != nil }

var ErrKeyNotFound = errors.New("key not found")

func (f *File) Get(key []byte) ([]byte, error) {
	ref := f.keymap.get(key)
	if ref == nil {
		return nil, fmt.Errorf("%w: %q", ErrKeyNotFound, key)
	}
	return f.readRowValue(ref)
}

func (f *File) readRowValue(ref *byteRange) ([]byte, error) {
	_, err := f.r.Seek(int64(ref.offset), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seek row start: %w", err)
	}
	_, _, _, value, err := DecodeRowFromReader(f.r)
	if err != nil {
		return nil, fmt.Errorf("decode row: %w", err)
	}
	return value, nil
}

func (f *File) WalkPrefix(prefix []byte, callback func(key []byte) error) error {
	return f.keymap.walk(prefix, func(currentKey []byte, _ *byteRange) error { return callback(currentKey) })
}

func (f *File) WalkPrefixKeyValue(prefix []byte, callback func(key, value []byte) error) error {
	return f.keymap.walk(prefix, func(currentKey []byte, ref *byteRange) error {
		currentValue, err := f.readRowValue(ref)
		if err != nil {
			return err
		}
		return callback(currentKey, currentValue)
	})
}
