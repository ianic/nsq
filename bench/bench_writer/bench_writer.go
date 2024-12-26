package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-nsq"
)

var (
	runfor     = flag.Duration("runfor", 10*time.Second, "duration of time to run")
	tcpAddress = flag.String("nsqd-tcp-address", "127.0.0.1:4150", "<addr>:<port> to connect to nsqd")
	topic      = flag.String("topic", "sub_bench", "topic to receive messages on")
	size       = flag.Int("size", 200, "size of messages")
	batchSize  = flag.Int("batch-size", 200, "batch size of messages")
	workers    = flag.Int("workers", runtime.GOMAXPROCS(0)/4, "number of writer workers (threads)")
	deadline   = flag.String("deadline", "", "deadline to start the benchmark run")
)

var totalMsgCount int64

func main() {
	flag.Parse()
	var wg sync.WaitGroup

	log.SetPrefix("[bench_writer] ")

	n := 0
	batch := make([][]byte, *batchSize)
	for k := range batch {
		msg := make([]byte, *size)
		for l := range msg {
			msg[l] = byte(n % 256)
			n += 1
		}
		batch[k] = msg
	}

	// log.Printf("starting %d workers", workers)

	goChan := make(chan int)
	rdyChan := make(chan int)
	for j := 0; j < *workers; j++ {
		wg.Add(1)
		go func() {
			pubWorker(*runfor, *tcpAddress, batch, *topic, rdyChan, goChan)
			wg.Done()
		}()
		<-rdyChan
	}

	if *deadline != "" {
		t, err := time.Parse("2006-01-02 15:04:05", *deadline)
		if err != nil {
			log.Fatal(err)
		}
		d := time.Until(t)
		log.Printf("sleeping until %s (%s)", t, d)
		time.Sleep(d)
	}

	start := time.Now()
	close(goChan)
	wg.Wait()
	end := time.Now()
	duration := end.Sub(start)
	tmc := atomic.LoadInt64(&totalMsgCount)
	log.Printf("duration: %s - %.03fmb/s - %.03fops/s - %.03fus/op messages: %d",
		duration.Round(time.Second),
		float64(tmc*int64(*size))/duration.Seconds()/1024/1024,
		float64(tmc)/duration.Seconds(),
		float64(duration/time.Microsecond)/float64(tmc),
		tmc)
}

func pubWorker(td time.Duration, tcpAddr string, batch [][]byte, topic string, rdyChan chan int, goChan chan int) {
	conn, err := net.DialTimeout("tcp", tcpAddr, time.Second)
	if err != nil {
		panic(err.Error())
	}
	conn.Write(nsq.MagicV2)
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriterSize(conn, (*size+4)**batchSize+128))
	ci := make(map[string]interface{})
	ci["client_id"] = "writer"
	ci["hostname"] = "writer"
	ci["user_agent"] = fmt.Sprintf("bench_writer/%s", nsq.VERSION)
	cmd, _ := nsq.Identify(ci)
	cmd.WriteTo(rw)
	rw.Flush()
	nsq.ReadResponse(rw)

	rdyChan <- 1
	<-goChan

	var msgCount int64
	endTime := time.Now().Add(td)

	for {
		cmd, _ := nsq.MultiPublish(topic, batch)
		_, err := cmd.WriteTo(rw)
		if err != nil {
			panic(err.Error())
		}
		err = rw.Flush()
		if err != nil {
			panic(err.Error())
		}
		resp, err := nsq.ReadResponse(rw)
		if err != nil {
			panic(err.Error())
		}
		frameType, data, err := nsq.UnpackResponse(resp)
		if err != nil {
			panic(err.Error())
		}
		if frameType == nsq.FrameTypeError {
			panic(string(data))
		}
		msgCount += int64(len(batch))
		if time.Now().After(endTime) {
			break
		}
	}
	atomic.AddInt64(&totalMsgCount, msgCount)
}
