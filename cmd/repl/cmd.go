package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ejuju/minidb/pkg/minidb"
)

type command struct {
	desc    string
	keyword string
	args    []string
	do      func(f *minidb.File, args ...string)
}

var commands = []*command{
	{
		keyword: "set",
		desc:    "sets a key-value pair",
		args:    []string{"key", "value"},
		do: func(f *minidb.File, args ...string) {
			err := f.Set([]byte(args[0]), []byte(args[1]))
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Printf("%q=%q\n", args[0], args[1])
		},
	},
	{
		keyword: "delete",
		desc:    "deletes a key-value pair",
		args:    []string{"key"},
		do: func(f *minidb.File, args ...string) {
			err := f.Delete([]byte(args[0]))
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Println("done")
		},
	},
	{
		keyword: "drop",
		desc:    "deletes all the key-value pairs",
		do: func(f *minidb.File, args ...string) {
			err := f.WalkPrefix([]byte{}, func(key []byte) error { return f.Delete(key) })
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Println("done")
		},
	},
	{
		keyword: "count",
		desc:    "reports the number of key-value pairs",
		do:      func(f *minidb.File, args ...string) { fmt.Println(f.Count(), "key-value pairs") },
	},
	{
		keyword: "has",
		desc:    "reports whether a key exists",
		args:    []string{"key"},
		do:      func(f *minidb.File, args ...string) { fmt.Println(f.Has([]byte(args[0]))) },
	},
	{
		keyword: "get",
		desc:    "reports the value associated with a given key",
		args:    []string{"key"},
		do: func(f *minidb.File, args ...string) {
			v, err := f.Get([]byte(args[0]))
			if err != nil {
				fmt.Println(err)
				return
			}
			fmt.Printf("%q=%q\n", args[0], v)
		},
	},
	{
		keyword: "all",
		desc:    "prints all key-value pairs",
		do: func(f *minidb.File, args ...string) {
			err := f.WalkPrefixKeyValue([]byte{}, func(key, value []byte) error {
				fmt.Printf("%q=%q\n", key, value)
				return nil
			})
			if err != nil {
				fmt.Println(err)
				return
			}
		},
	},
	{
		keyword: "walk",
		desc:    "prints all key-value pairs with the given prefix",
		args:    []string{"prefix"},
		do: func(f *minidb.File, args ...string) {
			err := f.WalkPrefixKeyValue([]byte(args[0]), func(key, value []byte) error {
				fmt.Printf("%q=%q\n", key, value)
				return nil
			})
			if err != nil {
				fmt.Println(err)
				return
			}
		},
	},
	{
		keyword: "fill",
		desc:    "adds key-value pairs to the database",
		args:    []string{"number"},
		do: func(f *minidb.File, args ...string) {
			num, err := strconv.Atoi(args[0])
			if err != nil {
				fmt.Println(err)
				return
			}
			for i := 0; i < num; i++ {
				key := fmt.Sprintf("%*d", len(args[0]), i)
				key = strings.ReplaceAll(key, " ", "0")
				err = f.Set([]byte(key), nil)
				if err != nil {
					fmt.Println(i, err)
					return
				}
			}
			fmt.Printf("added %d key-value pairs\n", num)
		},
	},
}
