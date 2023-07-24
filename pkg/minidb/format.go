package minidb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

const (
	_ byte = iota
	OpSet
	OpDelete
)

const (
	MaxKeyLength   = math.MaxUint8
	MaxValueLength = math.MaxUint16
)

var (
	ErrKeyTooLong   = errors.New("key too long")
	ErrValueTooLong = errors.New("value too long")
)

func FormatRow(op byte, k, v []byte) ([]byte, error) {
	if len(k) > MaxKeyLength {
		return nil, fmt.Errorf("%w: %d", ErrKeyTooLong, len(k))
	}
	if len(v) > MaxValueLength {
		return nil, fmt.Errorf("%w: %d", ErrValueTooLong, len(v))
	}
	var row []byte
	row = append(row, op)
	row = append(row, uint8(len(k)))
	row = binary.BigEndian.AppendUint16(row, uint16(len(v)))
	row = append(row, k...)
	row = append(row, v...)
	return row, nil
}

func DecodeRowFromReader(r io.Reader) (int, byte, []byte, []byte, error) {
	read := 0

	// Read op
	rawOp := [1]byte{}
	n, err := io.ReadFull(r, rawOp[:])
	if err != nil {
		return read, 0, nil, nil, fmt.Errorf("read op: %w", err)
	}
	read += n
	op := uint8(rawOp[0])

	// Read key-length
	rawKeyLength := [1]byte{}
	n, err = io.ReadFull(r, rawKeyLength[:])
	if err != nil {
		err = replaceEOFByUnexpectedEOF(err)
		return read, op, nil, nil, fmt.Errorf("read key-length: %w", err)
	}
	read += n
	keyLength := uint8(rawKeyLength[0])

	// Read value-length
	rawValueLength := make([]byte, 2)
	n, err = io.ReadFull(r, rawValueLength)
	if err != nil {
		err = replaceEOFByUnexpectedEOF(err)
		return read, op, nil, nil, fmt.Errorf("read value-length: %w", err)
	}
	read += n
	valueLength := binary.BigEndian.Uint16(rawValueLength)

	// Read key
	key := make([]byte, keyLength)
	n, err = io.ReadFull(r, key)
	if err != nil {
		err = replaceEOFByUnexpectedEOF(err)
		return read, op, nil, nil, fmt.Errorf("read key: %w", err)
	}
	read += n

	// Read value
	value := make([]byte, valueLength)
	n, err = io.ReadFull(r, value)
	if err != nil {
		err = replaceEOFByUnexpectedEOF(err)
		return read, op, nil, nil, fmt.Errorf("read value: %w", err)
	}
	read += n

	return read, op, key, value, nil
}

func replaceEOFByUnexpectedEOF(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}
