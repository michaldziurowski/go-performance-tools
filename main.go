package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"time"
)

func main() {
	report(os.Args[1], os.Args[2])
}

func report(in, out string) {
	inFile, _ := os.Open(in)
	defer inFile.Close()

	durations := [203221]int{}
	scanner := bufio.NewScanner(inFile)
	for scanner.Scan() {
		id, duration := newCarRecord(scanner.Bytes())
		durations[id] += duration
	}

	outFile, _ := os.Create(out)
	defer outFile.Close()

	writer := bufio.NewWriter(outFile)
	for id, duration := range durations {
		if duration > 0 {
			writer.WriteString(fmt.Sprintf("%d %d\r\n", id, duration))
		}
	}
}

func newCarRecord(b []byte) (int, int) {
	start := parseTime(b[:19])
	end := parseTime(b[20:39])
	id, _ := strconv.Atoi(string(b[40:48]))

	return id, int(end.Sub(start).Seconds())
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
