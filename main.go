package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"sync"
	"time"
)

func main() {
	report(os.Args[1], os.Args[2])
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Enter text: ")
	text, _ := reader.ReadString('\n')
	fmt.Println(text)
}

type record struct {
	ID       int
	Duration int
}

func report(in, out string) {
	inFile, _ := os.Open(in)
	defer inFile.Close()

	wg := &sync.WaitGroup{}

	jobsC := make(chan []byte, 100)
	durationsC := make(chan record, 100)
	durations := map[int]int{}
	doneC := make(chan bool)

	for i := 0; i < 100; i++ {
		go newCarRecord(jobsC, durationsC, wg)
	}

	go addDuration(durations, durationsC, doneC)

	scanner := bufio.NewScanner(inFile)
	for scanner.Scan() {
		wg.Add(1)
		jobsC <- scanner.Bytes()
	}
	close(jobsC)
	wg.Wait()
	close(durationsC)
	<-doneC

	outFile, _ := os.Create(out)
	defer outFile.Close()

	writer := bufio.NewWriter(outFile)
	for id, duration := range durations {
		if duration > 0 {
			writer.WriteString(fmt.Sprintf("%d %d\r\n", id, duration))
		}
	}
}

func addDuration(durations map[int]int, durationsC chan record, doneC chan bool) {
	for {
		if r, more := <-durationsC; more {
			durations[r.ID] += r.Duration
		} else {
			doneC <- true
			return
		}
	}
}

func newCarRecord(jobs <-chan []byte, durationsC chan record, wg *sync.WaitGroup) {
	for b := range jobs {
		start := parseTime(b[:19])
		end := parseTime(b[20:39])
		id := fromBytes(b[40:48])

		durationsC <- record{id, int(end.Sub(start).Seconds())}
		wg.Done()
	}
}

func parseTime(b []byte) time.Time {
	y := fromBytes(b[:4])
	m := time.Month(fromBytes(b[5:7]))
	d := fromBytes(b[8:10])
	h := fromBytes(b[11:13])
	mi := fromBytes(b[14:16])
	s := fromBytes(b[17:19])
	return time.Date(y, m, d, h, mi, s, 0, time.UTC)
}

func fromBytes(by []byte) int {
	result := 0
	idx := 0
	for i := len(by) - 1; i >= 0; i-- {
		result += int(by[i]-'0') * int(math.Pow10(idx))
		idx++
	}
	return result
}
