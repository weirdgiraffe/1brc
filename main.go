package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"golang.org/x/sync/errgroup"
)

func debugf(format string, args ...any) {
	// fmt.Printf(format, args...)
}

func ValueToFloat(value []byte) (float64, error) {
	s := unsafe.String(unsafe.SliceData(value), len(value))
	return strconv.ParseFloat(s, 64)
}

const valueSep = ';'

var ErrSeparatorNotFound = errors.New("separator not found")

type Item struct {
	name  []byte
	value float64
}

func ParseLine(line []byte) (out Item, err error) {
	sep := bytes.IndexByte(line, valueSep)
	if sep == -1 {
		return out, ErrSeparatorNotFound
	}

	out.name = line[:sep]
	out.value, err = ValueToFloat(line[sep+1:])
	return out, err
}

type Info struct {
	Name  string
	Min   float64
	Max   float64
	Sum   float64
	Count float64
}

func InfoFromItem(item Item) Info {
	return Info{
		Name:  string(item.name),
		Min:   item.value,
		Max:   item.value,
		Sum:   item.value,
		Count: 1,
	}
}
func (info *Info) Update(item Item) {
	if info.Min > item.value {
		info.Min = item.value
	}
	if info.Max < item.value {
		info.Max = item.value
	}
	info.Sum += item.value
	info.Count += 1
}

func (info *Info) Merge(other Info) {
	if info.Min > other.Min {
		info.Min = other.Min
	}
	if info.Max < other.Max {
		info.Max = other.Max
	}
	info.Sum += other.Sum
	info.Count += other.Count
}

type InfoStore struct {
	infos []Info
}

func NewInfoStore() *InfoStore {
	return &InfoStore{
		infos: make([]Info, 0, 1e3),
	}
}

func (store *InfoStore) At(i int) *Info {
	return &store.infos[i]
}

func (store *InfoStore) Merge(info Info) {
	n := len(store.infos)
	i := sort.Search(n, func(i int) bool {
		return strings.Compare(store.At(i).Name, info.Name) >= 0
	})

	if i == n {
		// all items are smaller than item, so add item to the end
		store.infos = append(store.infos, info)
		return
	}

	found := store.At(i)
	if found.Name == info.Name {
		found.Merge(info)
		return
	}

	// insert item at i
	store.infos = append(store.infos, Info{})
	copy(store.infos[i+1:], store.infos[i:n])
	store.infos[i] = info
}

func (store *InfoStore) Update(item Item) {
	n := len(store.infos)

	name := unsafe.String(unsafe.SliceData(item.name), len(item.name))

	i := sort.Search(n, func(i int) bool {
		return strings.Compare(store.At(i).Name, name) >= 0
	})

	if i == n {
		// all items are smaller than item, so add item to the end
		info := InfoFromItem(item)
		store.infos = append(store.infos, info)
		return
	}

	found := store.At(i)
	if found.Name == name {
		found.Update(item)
		return
	}

	// insert item at i
	info := InfoFromItem(item)
	store.infos = append(store.infos, Info{})
	copy(store.infos[i+1:], store.infos[i:n])
	store.infos[i] = info
}

func (store *InfoStore) Print() {
	return
	fmt.Print("{")
	for i, info := range store.infos {
		if i != 0 {
			fmt.Print(", ")
		}
		fmt.Printf("%s=%.1f/%.1f/%.1f",
			info.Name,
			info.Min,
			info.Sum/info.Count,
			info.Max)
	}
	fmt.Println("}")
}

func HandleLine(line []byte, store *InfoStore) error {
	item, err := ParseLine(line)
	if err != nil {
		return fmt.Errorf("failed to parse line: %w", err)
	}
	store.Update(item)

	line = line[:0]
	linePool.Put(&line)
	return nil
}

func DoWork(lines <-chan []byte, store *InfoStore) error {
	for line := range lines {
		err := HandleLine(line, store)
		if err != nil {
			return err
		}
	}
	return nil
}

var linePool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 100)
		return &b
	},
}

func HandleBuf(buf []byte, out chan<- []byte) (consumed int, err error) {
	for {
		le := bytes.IndexByte(buf[consumed:], '\n')
		if le == -1 {
			debugf("line end not found\n")
			return consumed, nil
		}

		bp := linePool.Get().(*[]byte)
		b := *bp
		b = append(b, buf[consumed:consumed+le]...)
		// b := make([]byte, le)
		// copy(b, buf[consumed:consumed+le])
		out <- b
		consumed += le + 1
	}
}

func MergeStores(l []*InfoStore) *InfoStore {
	store := NewInfoStore()
	for i := range l {
		for _, info := range l[i].infos {
			store.Merge(info)
		}
	}
	return store
}

func Solve(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	workers := runtime.GOMAXPROCS(-1)
	lines := make(chan []byte, 1000*workers)
	eg, _ := errgroup.WithContext(context.Background())

	stores := make([]*InfoStore, workers)
	for i := 0; i < workers; i++ {
		store := NewInfoStore()
		stores[i] = store
		eg.Go(func() error {
			return DoWork(lines, store)
		})
	}

	pageSize := os.Getpagesize()
	buf := make([]byte, pageSize)
	offt := 0
	for {
		var n int
		n, err = file.Read(buf[offt:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				close(lines)
				_ = eg.Wait()
				break
			}
			close(lines)
			_ = eg.Wait()
			return fmt.Errorf("failed to read file: %w", err)
		}

		consumed, err := HandleBuf(buf[:offt+n], lines)
		if err != nil {
			close(lines)
			_ = eg.Wait()
			return fmt.Errorf("failed to parse buf: %w", err)
		}
		copy(buf, buf[consumed:])
		offt += n - consumed
		debugf("offt=%d buf: %q\n", offt, buf[:offt])
	}

	store := MergeStores(stores)
	store.Print()
	return nil
}

func main() {
}
