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
	deadline   = flag.String("deadline", "", "deadline to start the benchmark run")
)

var totalMsgCount int64

func main() {
	flag.Parse()
	var wg sync.WaitGroup

	log.SetPrefix("[bench_writer] ")

	msg := make([]byte, *size)
	batch := make([][]byte, *batchSize)
	for i := range batch {
		batch[i] = msg
	}
	workers := runtime.GOMAXPROCS(0)
	//log.Printf("starting %d workers", workers)

	goChan := make(chan int)
	rdyChan := make(chan int)
	for j := 0; j < workers; j++ {
		wg.Add(1)
		go func() {
			pubWorker(*runfor, *tcpAddress, *batchSize, batch, *topic, rdyChan, goChan)
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

func pubWorker(td time.Duration, tcpAddr string, batchSize int, batch [][]byte, topic string, rdyChan chan int, goChan chan int) {
	conn, err := net.DialTimeout("tcp", tcpAddr, time.Second)
	if err != nil {
		panic(err.Error())
	}
	conn.Write(nsq.MagicV2)
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	ci := make(map[string]interface{})
	ci["client_id"] = "writer"
	ci["hostname"] = "writer"
	ci["user_agent"] = fmt.Sprintf("bench_writer/%s", nsq.VERSION)
	cmd, _ := nsq.Identify(ci)
	cmd.WriteTo(rw)
	rdyChan <- 1
	<-goChan
	rw.Flush()
	nsq.ReadResponse(rw)
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
