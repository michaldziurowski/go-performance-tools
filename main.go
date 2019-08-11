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

	lineLen := 50
	dataInLineLen := 48 // dont count \r\n
	noOfWorkers := 4
	cWork := make(chan []byte, 100)
	cDurations := make(chan map[int]float64, noOfWorkers)
	wg := sync.WaitGroup{}

	for i := 0; i < noOfWorkers; i++ {
		go func() {
			localDurations := make(map[int]float64)
			for dataRange := range cWork {
				for x := 0; x < len(dataRange); x += lineLen {
					line := string(dataRange[x : x+dataInLineLen])
					record := newCarRecord(line)
					duration := record.End.Sub(record.Start).Seconds()
					localDurations[record.ID] += duration
				}
			}
			cDurations <- localDurations
			wg.Done()
		}()
	}

	wg.Add(noOfWorkers)

	batchSize := 100000

	for {
		buf := make([]byte, batchSize)
		read, err := inFile.Read(buf)

		if err == io.EOF {
			break
		}

		cWork <- buf[:read]
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
