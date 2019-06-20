package main

import (
	"bufio"
	"fmt"
	"os"
	"time"
)

func main() {
	report(os.Args[1], os.Args[2])
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter text: ")
	text, _ := reader.ReadString('\n')
	fmt.Println(text)
}

func report(in, out string) {
	inFile, _ := os.Open(in)
	defer inFile.Close()

	durations := map[int]int{}
	scanner := bufio.NewScanner(inFile)
	for scanner.Scan() {
		id, duration := newCarRecord(scanner.Bytes())
		durations[id] += duration
	}

	outFile, _ := os.Create(out)
	defer outFile.Close()

	writer := bufio.NewWriter(outFile)
	for id, duration := range durations {
		writer.WriteString(fmt.Sprintf("%d %d\r\n", id, duration))
	}

	writer.Flush()
}

func newCarRecord(b []byte) (int, int) {
	start := parseTime(b[:19])
	end := parseTime(b[20:39])
	id := from8Bytes(b[40:48])

	return id, int(end.Sub(start).Seconds())
}

func parseTime(b []byte) time.Time {
	y := from4Bytes(b[:4])
	m := time.Month(from2Bytes(b[5:7]))
	d := from2Bytes(b[8:10])
	h := from2Bytes(b[11:13])
	mi := from2Bytes(b[14:16])
	s := from2Bytes(b[17:19])
	return time.Date(y, m, d, h, mi, s, 0, time.UTC)
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
