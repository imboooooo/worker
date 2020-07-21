package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"time"
)

const (
	//ErrorWorkerAlreadClosed this for error log for worker already close
	ErrorWorkerAlreadClosed = "worker already closed"
	// ErrorTimeoutProcess this for error timeout
	ErrorTimeoutProcess = "process timeout exceeded"
)

// ConfigWorkerWithBuffer this for worker with buffer
type ConfigWorkerWithBuffer struct {
	MessageSize int                // this for message buffer
	Worker      int                // this for number of worker
	FN          func(string) error // while process message
	Timeout     time.Duration      // this for set timeout execution
}

// NewWorkerWithBuffer this for new worker
func NewWorkerWithBuffer(cfg ConfigWorkerWithBuffer) *WorkerWithBuffer {
	var (
		worker = new(WorkerWithBuffer)
	)
	worker.signal = make(chan struct{})

	worker.poolMSG = make(chan string, cfg.MessageSize)

	// split message to each worker
	if cfg.Worker == 0 {
		cfg.Worker = 3
	}

	// set default timeout
	if cfg.Timeout == 0 {
		cfg.Timeout = time.Duration(10) * time.Minute
	}
	worker.timeout = cfg.Timeout

	// set timeout
	worker.fn = func(s string) error {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
		chErr := make(chan error)

		go func(ctx context.Context) {
			defer cancel()
			chErr <- cfg.FN(s)
		}(ctx)

		select {
		case <-ctx.Done():
			switch ctx.Err() {
			case context.DeadlineExceeded:
				return errors.New(ErrorTimeoutProcess)
			case context.Canceled:
				// context cancelled by force. whole process is complete
				return nil
			}
		case err := <-chErr:
			return err
		}
		return nil
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
	msg         []chan string
	signal      chan struct{}
	poolMSG     chan string
	tempMessage []string
	muMSG       sync.Mutex
	timeout     time.Duration
	fn          func(string) error
}

// addMessageToWorker this for add message to worker
func (w *WorkerWithBuffer) addMessageToWorker(msg string, pos int) int {
	n := pos
	for {
		select {
		case <-w.signal:
			w.tempMessage = append(w.tempMessage, msg)
			return n
		default:
			if n > len(w.msg)-1 {
				n = 0
			}
			if cap(w.msg[n]) <= len(w.msg[n]) {
				n++
			} else {
				w.msg[n] <- msg
				n++
				return n
			}
		}
	}
}

// dispatch this function we used for dispatch message
func (w *WorkerWithBuffer) dispatch() error {
	n := 0
	for {
		select {
		case <-w.signal:
			return nil
		case msg := <-w.poolMSG:
			n = w.addMessageToWorker(msg, n)

		}
	}
	return nil
}

// worker this function we used for worker
func (w *WorkerWithBuffer) worker(workerNumber int, msg chan string) {
	for {
		select {
		case payload := <-msg:
			func() {
				defer w.recover(fmt.Sprint("worker ", workerNumber))
				w.fn(payload)
			}()
		case <-w.signal:
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
			return errors.New(ErrorWorkerAlreadClosed)
		default:
			if len(w.poolMSG) < cap(w.poolMSG) {
				w.poolMSG <- payload
				return nil
			}
		}
		time.Sleep(1 * time.Second)
	}
}

// cleanUpMessage this function we used clean up message
func (w *WorkerWithBuffer) cleanUpMessage() {

	log.Printf("clean up message on worker")

	wg := &sync.WaitGroup{}
	/// clea from  pool message
	for i := len(w.poolMSG); i > 0; i-- {
		wg.Add(1)
		go func(fWG *sync.WaitGroup, payload string) {
			defer func() {
				fWG.Done()
				w.recover(fmt.Sprint("clean up pool message"))
			}()

			w.fn(payload)
		}(wg, <-w.poolMSG)
	}

	for n := 0; n < len(w.msg); n++ {
		for i := len(w.msg[n]); i > 0; i-- {
			wg.Add(1)
			go func(fWG *sync.WaitGroup, payload string) {
				defer func() {
					fWG.Done()
					w.recover(fmt.Sprintf("clean up message pool "))
				}()
				w.fn(payload)
			}(wg, <-w.msg[n])
		}
	}
	for _, temp := range w.tempMessage {
		wg.Add(1)
		go func(fWG *sync.WaitGroup, payload string) {
			defer func() {
				fWG.Done()
				w.recover(fmt.Sprintf("clean up message pool "))
			}()
			w.fn(payload)
		}(wg, temp)
	}
	wg.Wait()
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
	w.cleanUpMessage()
	for n := 0; n < len(w.msg); n++ {
		close(w.msg[n])
	}

}

// recover this for recover
func (w *WorkerWithBuffer) recover(eventName string) {
	if r := recover(); r != nil {
		log.Printf("%s recovered err: %v stack_trace: %v \n", eventName, r, string(debug.Stack()))

	}
}

// Stop this for stop process
func (w *WorkerWithBuffer) Stop() {
	close(w.signal)
}
