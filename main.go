package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
)

type Info struct {
	Min   float64
	Max   float64
	Total float64
	N     int
}

func Solve(filename string) error {
	memHint := int(1e4)
	var dict = make(map[string]*Info, memHint)

	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	lines := 0
	pageSize := os.Getpagesize()
	buf := make([]byte, pageSize)
	fofft := 0
	for {
		n, err := file.Read(buf[fofft:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("failed to read file: %w", err)
		}

		// 		fmt.Printf("read %d bytes\n", n)
		offt := 0
		for {
			le := bytes.IndexByte(buf[offt:fofft+n], '\n')
			if le == -1 {
				// 				fmt.Printf("line end not found, offt=%d\n", offt)
				break
			}
			// 			fmt.Printf("le=%d %q\n", le, buf[offt:offt+le])
			i := bytes.IndexByte(buf[offt:offt+le], ';')
			// 			fmt.Printf("separator at %d\n", i)
			name := buf[offt : i+offt]
			// 			fmt.Printf("name: %q\n", name)
			bvalue := buf[i+offt+1 : offt+le]
			// 			fmt.Printf("bvalue: %q\n", bvalue)
			value, err := strconv.ParseFloat(string(bvalue), 64)
			if err != nil {
				return fmt.Errorf("failed to parse float %q: %w", string(bvalue), err)
			}

			info := dict[string(name)]
			if info == nil {
				info = &Info{
					Min:   value,
					Max:   value,
					Total: value,
					N:     1,
				}
				dict[string(name)] = info
				offt += le + 1
				// 				fmt.Printf("offt = %d\n", offt)
				continue
			}

			if info.Min > value {
				info.Min = value
			}
			if info.Max < value {
				info.Max = value
			}
			info.Total += value
			info.N += 1

			lines += 1
			offt += le + 1
			// fmt.Printf("offt = %d\n", offt)
		}
		// fmt.Printf("lines = %d\r", lines)
		copy(buf, buf[offt:])
		fofft += n - offt
		// fmt.Printf("fofft=%d buf: %q\n", fofft, buf[:offt])
	}
}

func main() {
}
