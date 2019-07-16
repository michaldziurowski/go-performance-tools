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

	durations := make(map[int]float64)
	var wg sync.WaitGroup
	var mu sync.Mutex

	noOfWorkers := 4
	cWork := make(chan []string, 100)

	for i := 0; i < noOfWorkers; i++ {
		go func() {
			for l := range cWork {
				for x := 0; x < len(l); x++ {
					line := l[x]
					if line != "" {
						record := newCarRecord(line)
						duration := record.End.Sub(record.Start).Seconds()
						mu.Lock()
						durations[record.ID] += duration
						mu.Unlock()
					} else {
						fmt.Printf("Received empty line\n")
					}
				}
			}

			wg.Done()
		}()
	}

	wg.Add(noOfWorkers)

	linesLen := len(lines)
	divider := 100000
	times := int(linesLen / divider)

	for y := 0; y < times; y++ {
		cWork <- lines[y*divider : (y*divider)+divider]
	}

	cWork <- lines[times*divider:]

	close(cWork)

	wg.Wait()

	var sb strings.Builder
	fmt.Printf("durations : %v\n", len(durations))
	for id, duration := range durations {
		sb.WriteString(fmt.Sprintf("%d %.0f\r\n", id, duration))
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
