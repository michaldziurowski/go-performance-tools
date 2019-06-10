package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	report(os.Args[1], os.Args[2])
}

func report(in, out string) {
	inFile, _ := os.Open(in)
	defer inFile.Close()

	durations := make(map[int]float64)
	scanner := bufio.NewScanner(inFile)

	for scanner.Scan() {
		record := newCarRecord(scanner.Text())
		duration := record.End.Sub(record.Start).Seconds()
		if _, ok := durations[record.ID]; ok {
			durations[record.ID] += duration
		} else {
			durations[record.ID] = duration
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error scanning file %s %v\n", in, err)
		return
	}

	outFile, _ := os.Create(out)
	defer outFile.Close()

	writer := bufio.NewWriter(outFile)
	for id, duration := range durations {
		writer.WriteString(fmt.Sprintf("%d %.0f\r\n", id, duration))
	}
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
