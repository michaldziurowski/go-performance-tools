package main

import (
	"fmt"
	"io"
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
	inFile, _ := os.Open(in)
	defer inFile.Close()

	durations := make(map[int]float64)
	var wg sync.WaitGroup

	noOfWorkers := 4
	cWork := make(chan []byte, 100)
	cDurations := make(chan map[int]float64, noOfWorkers)

	for i := 0; i < noOfWorkers; i++ {
		go func() {
			locdurations := make(map[int]float64)
			for l := range cWork {
				for x := 0; x < len(l); x += 50 {
					line := string(l[x : x+48]) // 48 because we dont want to add \r\n
					if line != "" {
						record := newCarRecord(line)
						duration := record.End.Sub(record.Start).Seconds()
						locdurations[record.ID] += duration
					} else {
						fmt.Printf("Received empty line\n")
					}
				}
			}
			cDurations <- locdurations
			wg.Done()
		}()
	}

	wg.Add(noOfWorkers)

	divider := 100000
	readCount := 0

	for {
		buf := make([]byte, divider)

		read, err := inFile.Read(buf)
		if err == io.EOF {
			break
		}

		cWork <- buf[:read]
		readCount += read
	}

	close(cWork)

	wg.Wait()

	for i := 0; i < noOfWorkers; i++ {
		d := <-cDurations
		for id, duration := range d {
			durations[id] += duration
		}
	}

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
