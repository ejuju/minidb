package minidb

import (
	"errors"
	"fmt"
)

type byteRange struct {
	offset int
	length int
}

type keymap struct {
	root *keymapNode
}

type keymapNode struct {
	children [256]*keymapNode
	ref      *byteRange
}

func (km *keymap) set(k []byte, ref *byteRange) {
	n := km.root
	for _, c := range k {
		if n.children[c] == nil {
			n.children[c] = &keymapNode{}
		}
		n = n.children[c]
	}
	n.ref = ref
}

func (km *keymap) delete(k []byte) {
	n := km.root
	for _, c := range k {
		if n.children[c] == nil {
			return
		}
		n = n.children[c]
	}
	n.ref = nil
}

func (km *keymap) get(k []byte) *byteRange {
	n := km.root
	for _, c := range k {
		if n.children[c] == nil {
			return nil
		}
		n = n.children[c]
	}
	return n.ref
}

var ErrPrefixNotFound = errors.New("prefix not found")

func (km *keymap) walk(prefix []byte, callback func([]byte, *byteRange) error) error {
	n := km.root
	for _, c := range prefix {
		if n.children[c] == nil {
			return fmt.Errorf("%w: %q", ErrPrefixNotFound, prefix)
		}
		n = n.children[c]
	}
	return n.walk(prefix, callback)
}

func (n *keymapNode) walk(prefix []byte, callback func([]byte, *byteRange) error) error {
	if n.ref != nil {
		err := callback(prefix, n.ref)
		if err != nil {
			return err
		}
	}
	for childChar, child := range n.children {
		if child == nil {
			continue
		}
		child.walk(append(prefix, byte(childChar)), callback)
	}
	return nil
}
