package worker

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// NewWorkerWithBuffer this for new worker
func NewWorkerWithBuffer(numberWorker int, fn func(string) error) *WorkerWithBuffer {
	var (
		worker = new(WorkerWithBuffer)
	)
	worker.signal = make(chan struct{})
	worker.fn = fn
	worker.poolMSG = make(chan string, numberWorker)
	worker.msg = []chan string{
		make(chan string, int(numberWorker/3)),
		make(chan string, int(numberWorker/3)),
		make(chan string, int(numberWorker/3)+int(numberWorker%3)),
	}

	return worker

}

// WorkerWithBuffer this struct for worker
type WorkerWithBuffer struct {
	msg     []chan string
	signal  chan struct{}
	poolMSG chan string
	muMSG   sync.Mutex
	fn      func(string) error
}

// dispatch this function we used for dispatch message
func (w *WorkerWithBuffer) dispatch() error {
	n := 0
	for {
		select {
		case msg := <-w.poolMSG:
			w.msg[n] <- msg
			if n >= 2 {
				n = 0
			} else {
				n++
			}
		case <-w.signal:
			return nil
		}
	}
	return nil
}

// worker this function we used for worker
func (w *WorkerWithBuffer) worker(msg chan string) {
	for {
		select {
		case payload := <-msg:
			go func() {
				defer func() {
					if r := recover(); r != nil {
						fmt.Println("Recovered in f", r)
					}
				}()
				w.fn(payload)
			}()
		case <-w.signal:
			for i := len(msg); i > 0; i++ {
				go func(payload string) {
					defer func() {
						if r := recover(); r != nil {
							fmt.Println("Recovered in f", r)
						}
					}()

					w.fn(payload)
				}(<-msg)
			}
			log.Println("worker close signal")
			return
		}
	}
}

//SendJob this for send job
func (w *WorkerWithBuffer) SendJob(payload string) {

	for {
		select {
		case <-w.signal:
			return
		default:
			if len(w.poolMSG) < cap(w.poolMSG) {
				w.poolMSG <- payload
				return
			}
		}

		time.Sleep(1 * time.Second)
	}
}

// Start this for wait process
func (w *WorkerWithBuffer) Start() {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func(fwg *sync.WaitGroup) {
		defer fwg.Done()
		w.dispatch()
	}(wg)

	for n := 0; n < cap(w.msg); n++ {
		wg.Add(1)
		go func(fwg *sync.WaitGroup, msgIndex int) {
			defer fwg.Done()
			w.worker(w.msg[msgIndex])
		}(wg, n)
	}
	wg.Wait()
	for n := 0; n < cap(w.msg); n++ {
		close(w.msg[n])
	}

}

// Stop this for stop process
func (w *WorkerWithBuffer) Stop() {
	close(w.signal)
}
