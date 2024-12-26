package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-nsq"
)

var (
	runfor     = flag.Duration("runfor", 10*time.Second, "duration of time to run")
	tcpAddress = flag.String("nsqd-tcp-address", "127.0.0.1:4150", "<addr>:<port> to connect to nsqd")
	__size     = flag.Int("size", 200, "size of messages")
	topic      = flag.String("topic", "sub_bench", "topic to receive messages on")
	channel    = flag.String("channel", "ch", "channel to receive messages on")
	deadline   = flag.String("deadline", "", "deadline to start the benchmark run")
	rdy        = flag.Int("rdy", 2500, "RDY count to use")
	workers    = flag.Int("workers", runtime.GOMAXPROCS(0)/2, "number of writer workeres (threads)")
)

var totalMsgCount uint64
var totalMsgBytes uint64

func main() {
	flag.Parse()
	var wg sync.WaitGroup

	log.SetPrefix("[bench_reader] ")

	goChan := make(chan int)
	rdyChan := make(chan int)
	//workers := runtime.GOMAXPROCS(0)
	//workers = 6
	// log.Printf("starting %d workers", workers)

	for j := 0; j < *workers; j++ {
		wg.Add(1)
		go func(id int) {
			subWorker(*runfor, *tcpAddress, *topic, *channel, rdyChan, goChan, id)
			wg.Done()
		}(j)
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
	tmc := atomic.LoadUint64(&totalMsgCount)
	tmb := atomic.LoadUint64(&totalMsgBytes)
	log.Printf("duration: %s - %.03fmb/s - %.03fops/s - %.03fus/op messages: %d",
		duration.Round(time.Second),
		float64(tmb)/duration.Seconds()/1024/1024,
		float64(tmc)/duration.Seconds(),
		float64(duration/time.Microsecond)/float64(tmc),
		tmc)
}

func subWorker(td time.Duration, tcpAddr string, topic string, channel string, rdyChan chan int, goChan chan int, id int) {
	conn, err := net.DialTimeout("tcp", tcpAddr, 5*time.Second)
	if err != nil {
		panic(err.Error())
	}
	_ = conn.SetReadDeadline(time.Now().Add(td + 5*time.Second))

	conn.Write(nsq.MagicV2)
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	ci := make(map[string]interface{})
	ci["client_id"] = "reader"
	ci["hostname"] = "reader"
	ci["user_agent"] = fmt.Sprintf("bench_reader/%s", nsq.VERSION)
	cmd, _ := nsq.Identify(ci)
	cmd.WriteTo(rw)
	nsq.Subscribe(topic, channel).WriteTo(rw)
	rdyChan <- 1
	<-goChan
	nsq.Ready(*rdy).WriteTo(rw)
	rw.Flush()
	// two OK responses
	_, err = nsq.ReadResponse(rw)
	if err != nil {
		panic(err.Error())
	}
	_, err = nsq.ReadResponse(rw)
	if err != nil {
		panic(err.Error())
	}

	var msgCount uint64
	var msgBytes uint64
	done := make(chan struct{})
	go func() {
		time.Sleep(td)
		close(done)
		//time.Sleep(100 * time.Millisecond)
		//conn.Close()
	}()
	var closing = false

	// clean close will not leave in flight messages
	// it will send fin for each message
	// unclean can lave something in flight and force server to requeue
	const clean_close = true
out:
	for {
		resp, err := nsq.ReadResponse(rw)
		if err != nil {
			if strings.Contains(err.Error(), "use of closed network connection") {
				log.Print(err)
				break
			}
			if os.IsTimeout(err) || errors.Is(err, io.EOF) {
				// log.Print(err)
				break out
			}
			log.Printf("messages count: %d worker %d", msgCount, id)
			panic(err.Error())
		}
		if len(resp) == 0 {
			log.Printf("EOF")
			break
		}
		frameType, data, err := nsq.UnpackResponse(resp)
		if err != nil {
			panic(err.Error())
		}
		if frameType == nsq.FrameTypeError {
			if string(data) == "E_FIN_FAILED" || string(data) == "E_INVALID" {
				log.Println(string(data))
			} else {
				panic(string(data))
			}
			continue
		} else if frameType == nsq.FrameTypeResponse {
			if string(data) == "CLOSE_WAIT" {
				// log.Printf("close wait received %s", data)
				break out
			}
			continue
		}
		msg, err := nsq.DecodeMessage(data)
		if err != nil {
			panic(err.Error())
		}
		msgBytes += uint64(len(msg.Body))
		nsq.Finish(msg.ID).WriteTo(rw)
		msgCount++

		if !closing {
			select {
			case <-done:
				if clean_close {
					nsq.StartClose().WriteTo(rw)
					closing = true
				} else {
					break out
				}
			default:
			}
		}
	}
	rw.Flush()
	conn.Close()
	atomic.AddUint64(&totalMsgCount, msgCount)
	atomic.AddUint64(&totalMsgBytes, msgBytes)
}
