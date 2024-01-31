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

const (
	valueSep = ';'
	endLine  = '\n'
)

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
		return fmt.Errorf("failed to parse line %q: %w", line, err)
	}
	store.Update(item)
	return nil
}

func HandlePage(page *Page, store *InfoStore) error {
	buf := page.b[:page.n]
	consumed := 0
	for {
		le := bytes.IndexByte(buf[consumed:], '\n')
		if le == -1 {
			break
		}

		err := HandleLine(buf[consumed:consumed+le], store)
		if err != nil {
			return err
		}
		consumed += le + 1
	}

	err := HandleLine(buf[consumed:], store)
	if err != nil {
		return err
	}

	page.n = pageSize
	pagePool.Put(page)
	return nil
}

func DoWork(pages <-chan *Page, store *InfoStore) error {
	for page := range pages {
		err := HandlePage(page, store)
		if err != nil {
			return err
		}
	}
	return nil
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

var pageSize = os.Getpagesize()
var pagePool = sync.Pool{
	New: func() any {
		p := Page{
			b: make([]byte, pageSize),
			n: pageSize,
		}
		return &p
	},
}

type Page struct {
	b []byte
	n int
}

func Solve(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	workers := runtime.GOMAXPROCS(-1)
	pages := make(chan *Page, 10*workers)
	eg, ectx := errgroup.WithContext(context.Background())

	stores := make([]*InfoStore, workers)
	for i := 0; i < workers; i++ {
		store := NewInfoStore()
		stores[i] = store
		eg.Go(func() error {
			return DoWork(pages, store)
		})
	}

	eg.Go(func() (err error) {
		defer close(pages)
		prefix := make([]byte, 0, 100)
		var n int
		for {
			p := pagePool.Get().(*Page)
			buf := p.b

			copy(buf, prefix)
			offt := len(prefix)
			prefix = prefix[:0]

			n, err = file.Read(buf[offt:])
			if err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
			}

			i := bytes.LastIndexByte(buf[:offt+n], endLine)
			if i != -1 {
				prefix = append(prefix, buf[i+1:offt+n]...)
				p.n = i
			}

			select {
			case <-ectx.Done():
				return ectx.Err()
			case pages <- p:
			}
		}
	})

	err = eg.Wait()
	if err != nil {
		return fmt.Errorf("failed to solve: %w", err)
	}

	store := MergeStores(stores)
	store.Print()
	return nil
}

func main() {
	filename := os.Args[1]
	if err := Solve(filename); err != nil {
		fmt.Fprintf(os.Stderr, "failed to solve: %v\n", err)
		os.Exit(1)
	}
}
