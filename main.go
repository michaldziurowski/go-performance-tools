package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"time"
)

func main() {
	// report(os.Args[1], os.Args[2])
	report("data.txt", "summary.txt")
}

func report(in, out string) {
	inFile, _ := os.Open(in)
	defer inFile.Close()

	durations := make(map[int]float64)
	scanner := bufio.NewScanner(inFile)

	for scanner.Scan() {
		record := newCarRecord(scanner.Bytes())
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

	writer.Flush()
}

type carRecord struct {
	Start time.Time
	End   time.Time
	ID    int
}

func newCarRecord(b []byte) carRecord {

	start := parseTime(b[:19])
	end := parseTime(b[20:39])
	id, _ := strconv.Atoi(string(b[40:48]))

	return carRecord{start, end, id}
}

func parseTime(b []byte) time.Time {
	y := from4digit(b[:4])
	m := time.Month(from2digit(b[5:7]))
	d := from2digit(b[8:10])
	h := from2digit(b[11:13])
	mi := from2digit(b[14:16])
	s := from2digit(b[17:19])
	return time.Date(y, m, d, h, mi, s, 0, time.UTC)
}

func from4digit(b []byte) int {
	return int(b[0]-'0')*1000 + int(b[1]-'0')*100 + int(b[2]-'0')*10 + int(b[3]-'0')
}

func from2digit(b []byte) int {
	return int(b[0]-'0')*10 + int(b[1]-'0')
}
