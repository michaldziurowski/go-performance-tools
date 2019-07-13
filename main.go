package main

import (
	"bufio"
	"fmt"
	"os"

	"golang.org/x/exp/mmap"
)

var daysToMonth365 = []int{0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365}
var daysToMonth366 = []int{0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366}

func main() {
	report(os.Args[1], os.Args[2])
}

func report(in, out string) {
	inFile, _ := os.Open(in)
	defer inFile.Close()

	cDurations := make(chan []int)
	durations := make([]int, 2010000)

	reader, err := mmap.Open(in)

	if err != nil {
		fmt.Printf("%v", err)
	}

	defer reader.Close()

	noOfLines := reader.Len() / 50

	noOfWorkers := 4
	workerBatch := noOfLines / noOfWorkers

	for j := 0; j < noOfWorkers; j++ {
		if j == (noOfWorkers - 1) {
			go calculateBatch(reader, cDurations, j*workerBatch, workerBatch+(noOfLines%noOfWorkers))
		} else {
			go calculateBatch(reader, cDurations, j*workerBatch, workerBatch)
		}

	}

	for i := 0; i < noOfWorkers; i++ {
		workerDurations := <-cDurations

		for id, duration := range workerDurations {
			durations[id] += duration
		}
	}

	outFile, _ := os.Create(out)
	defer outFile.Close()

	writer := bufio.NewWriter(outFile)
	for id, duration := range durations {
		if duration != 0 {
			writer.WriteString(fmt.Sprintf("%d %d\r\n", id, duration))
		}
	}

	writer.Flush()
}

func calculateBatch(reader *mmap.ReaderAt, results chan []int, start int, noOfLines int) {
	durations := make([]int, 2010000)
	line := make([]byte, 50)
	for i := start; i < (noOfLines + start); i++ {
		num, err := reader.ReadAt(line, int64(50*i))
		if num < 50 || err != nil {
			fmt.Printf("read %v bytes", num)
		}
		id, duration := newCarRecord(line)
		durations[id] += duration
	}

	results <- durations
}

func newCarRecord(b []byte) (int, int) {
	start := b[:19]
	end := b[20:39]
	id := from8Bytes(b[40:48])

	return id, (parseToSeconds(end) - parseToSeconds(start))
}

func parseToSeconds(b []byte) int {
	year := from4Bytes(b[:4])
	month := from2Bytes(b[5:7])
	day := from2Bytes(b[8:10])
	hour := from2Bytes(b[11:13])
	min := from2Bytes(b[14:16])
	sec := from2Bytes(b[17:19])

	var leap = year%4 == 0 && (year%100 != 0 || year%400 == 0)
	var days []int
	if leap {
		days = daysToMonth366
	} else {
		days = daysToMonth365
	}

	y := year - 1
	n := y*365 + y/4 - y/100 + y/400 + days[month-1] + day - 1

	return n*86400 + hour*3600 + min*60 + sec

}

func from2Bytes(by []byte) int {
	return int(by[0]-'0')*10 + int(by[1]-'0')
}

func from4Bytes(by []byte) int {
	return int(by[0]-'0')*1000 + int(by[1]-'0')*100 + int(by[2]-'0')*10 + int(by[3]-'0')
}

func from8Bytes(by []byte) int {
	return int(by[0]-'0')*10000000 + int(by[1]-'0')*1000000 + int(by[2]-'0')*1000000 + int(by[3]-'0')*10000 + int(by[4]-'0')*1000 + int(by[5]-'0')*100 + int(by[6]-'0')*10 + int(by[7]-'0')
}
