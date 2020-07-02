package worker

import (
	"context"
	"log"
	"sync"
	"time"
)

// ConfigWorkerWithBuffer this for worker with buffer
type ConfigWorkerWithBuffer struct {
	MessageSize int                // this for message buffer
	Worker      int                // this for number of worker
	FN          func(string) error // while process message
}

// NewWorkerWithBuffer this for new worker
func NewWorkerWithBuffer(cfg ConfigWorkerWithBuffer) *WorkerWithBuffer {
	var (
		worker = new(WorkerWithBuffer)
	)
	worker.signal = make(chan struct{})
	worker.fn = cfg.FN

	worker.poolMSG = make(chan string, cfg.MessageSize)

	// split message to each worker
	if cfg.Worker == 0 {
		cfg.Worker = 3
	}
	worker.msg = []chan string{}
	for i := 0; i < cfg.Worker; i++ {
		if i == (cfg.Worker - 1) {
			worker.msg = append(worker.msg, make(chan string, int(cfg.MessageSize/cfg.Worker)+int(cfg.MessageSize%cfg.Worker)))
			continue
		}
		worker.msg = append(worker.msg, make(chan string, int(cfg.MessageSize/cfg.Worker)))
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
			if n >= len(w.msg)-1 {
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
func (w *WorkerWithBuffer) worker(workerNumber int, msg chan string) {
	for {
		select {
		case payload := <-msg:
			go func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("worker %d recovered err:%v\n", workerNumber, r)
					}
				}()
				w.fn(payload)
			}()
		case <-w.signal:
			for i := len(msg); i > 0; i++ {
				go func(payload string) {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("worker %d recovered err:%v\n", workerNumber, r)
						}
					}()
					w.fn(payload)
				}(<-msg)
			}
			log.Printf("close worker %d \n", workerNumber)
			return
		}
	}
}

//SendJob this for send job
func (w *WorkerWithBuffer) SendJob(ctx context.Context, payload string) error {

	for {
		select {
		case <-w.signal:
			log.Print("close send job message")
			return nil
		default:
			if len(w.poolMSG) < cap(w.poolMSG) {
				w.poolMSG <- payload
				return nil
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

	for n := 0; n < len(w.msg); n++ {
		wg.Add(1)
		go func(fwg *sync.WaitGroup, msgIndex int) {
			defer fwg.Done()
			w.worker(msgIndex, w.msg[msgIndex])
		}(wg, n)
	}
	wg.Wait()
	for n := 0; n < len(w.msg); n++ {
		close(w.msg[n])
	}

}

// Stop this for stop process
func (w *WorkerWithBuffer) Stop() {
	close(w.signal)
}
