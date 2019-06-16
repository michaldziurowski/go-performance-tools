package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	report(os.Args[1], os.Args[2])
}

func report(in, out string) {
	bytes, _ := ioutil.ReadFile(in)
	content := string(bytes)
	lines := strings.Split(content, "\r\n")

	durations := NewRegularIntMap()
	wg := &sync.WaitGroup{}
	for _, line := range lines {
		wg.Add(1)
		if line != "" {
			go func(l string) {
				record := newCarRecord(l)
				duration := record.End.Sub(record.Start).Seconds()
				durations.IncreaseStore(record.ID, int(duration))
				wg.Done()
			}(line)
		} else {
			wg.Done()
		}
	}

	wg.Wait()
	var sb strings.Builder
	for id, duration := range durations.internal {
		sb.WriteString(fmt.Sprintf("%d %d\r\n", id, duration))
	}

	ioutil.WriteFile(out, []byte(sb.String()), 0644)
}

type carRecord struct {
	Start time.Time
	End   time.Time
	ID    int
}

func newCarRecord(line string) carRecord {
	parts := strings.Split(line, " ")

	start, _ := time.Parse("2006-01-02T15:04:05", parts[0])
	end, _ := time.Parse("2006-01-02T15:04:05", parts[1])
	id, _ := strconv.Atoi(parts[2])

	return carRecord{start, end, id}
}

type RegularIntMap struct {
	sync.Mutex
	internal map[int]int
}

func NewRegularIntMap() *RegularIntMap {
	return &RegularIntMap{
		internal: make(map[int]int),
	}
}

func (rm *RegularIntMap) IncreaseStore(key, value int) {
	rm.Lock()
	rm.internal[key] += value
	rm.Unlock()
}
