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
	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}

	noOfWorkers := 4
	cWork := make(chan []string, 100)

	for i := 0; i < noOfWorkers; i++ {
		go func() {
			for l := range cWork {
				for i := 0; i < len(l); i++ {
					line := l[i]
					if line != "" {
						record := newCarRecord(line)
						duration := record.End.Sub(record.Start).Seconds()
						mu.Lock()
						durations[record.ID] += duration
						mu.Unlock()
					}
				}
			}

			wg.Done()
		}()
	}

	wg.Add(noOfWorkers)

	linesSlice := make([]string, 100)
	counter := 0

	for _, line := range lines {
		if counter == 100 {
			cWork <- linesSlice
			counter = 0
		}
		linesSlice[counter] = line
		counter++
	}

	if counter != 0 {
		cWork <- linesSlice[0:counter]
	}

	close(cWork)

	wg.Wait()

	var sb strings.Builder

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
