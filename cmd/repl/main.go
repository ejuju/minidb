package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ejuju/minidb/pkg/minidb"
)

func main() {
	fpath := filepath.Join(os.TempDir(), "minidb")
	if len(os.Args) == 2 {
		fpath = os.Args[1]
	}
	startOpenFile := time.Now()
	f, err := minidb.Open(fpath)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	fmt.Printf("Opened database in %.4f seconds\n\n", time.Since(startOpenFile).Seconds())

	bufs := bufio.NewScanner(os.Stdin)
	for bufs.Scan() {
		handleCommand(f, bufs.Text())
		fmt.Print("\n")
	}
	if err := bufs.Err(); err != nil {
		panic(err)
	}
}

func handleCommand(f *minidb.File, line string) {
	parts := strings.Split(line, " ")
	keyword := parts[0]

	// Find and exec command
	for _, cmd := range commands {
		if cmd.keyword == keyword {
			var args []string
			if len(cmd.args) > 0 {
				if len(parts)-1 != len(cmd.args) {
					fmt.Printf("%q needs %d argument(s): %s\n", keyword, len(cmd.args), strings.Join(cmd.args, ", "))
					return
				}
				args = parts[1:]
			}
			cmd.do(f, args...)
			return
		}
	}

	// Show help if command not found
	fmt.Println("Available commands:")
	for _, cmd := range commands {
		fmt.Printf("\t\033[033m%-8s\033[0m %s", cmd.keyword, cmd.desc)
		if len(cmd.args) > 0 {
			fmt.Printf(" (expects arguments: %s)", strings.Join(cmd.args, ", "))
		}
		fmt.Print("\n")
	}
}
