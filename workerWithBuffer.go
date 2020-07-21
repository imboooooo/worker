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
	// ErrorWorkerClose this for close worker
	ErrorWorkerClose = "worker already closed"
)

// ConfigWorkerWithBuffer this for worker with buffer
type ConfigWorkerWithBuffer struct {
	MessageSize int                                 // this for message buffer
	Worker      int                                 // this for number of worker
	FN          func(context.Context, string) error // while process message
	Timeout     time.Duration
}

// workerMessage worker message
type workerMessage struct {
	ctx context.Context
	msg string
}

// NewWorkerWithBuffer this for new worker
func NewWorkerWithBuffer(cfg ConfigWorkerWithBuffer) *WorkerWithBuffer {
	var (
		worker = new(WorkerWithBuffer)
	)
	worker.signal = make(chan struct{})

	worker.poolMSG = make(chan workerMessage, cfg.MessageSize)

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
	worker.fn = func(i context.Context, s string) error {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			time.Duration(cfg.Timeout))
		chErr := make(chan error)
		go func(ctx context.Context) {
			defer cancel()
			chErr <- cfg.FN(i, s)
		}(ctx)
		select {
		case <-ctx.Done():
			switch ctx.Err() {
			case context.DeadlineExceeded:
				fmt.Println("process timeout exceeded")
				return errors.New("context timeout exceeded")
			case context.Canceled:
				// context cancelled by force. whole process is complete
				return nil
			}
		case err := <-chErr:
			return err
		}
		return nil
	}

	// set timeout

	worker.msg = []chan workerMessage{}
	for i := 0; i < cfg.Worker; i++ {
		if i == (cfg.Worker - 1) {
			worker.msg = append(worker.msg, make(chan workerMessage, int(cfg.MessageSize/cfg.Worker)+int(cfg.MessageSize%cfg.Worker)))
			continue
		}
		worker.msg = append(worker.msg, make(chan workerMessage, int(cfg.MessageSize/cfg.Worker)))
	}

	return worker

}

// WorkerWithBuffer this struct for worker
type WorkerWithBuffer struct {
	msg     []chan workerMessage
	signal  chan struct{}
	poolMSG chan workerMessage
	muMSG   sync.Mutex
	fn      func(context.Context, string) error
	timeout time.Duration
}

// dispatch this function we used for dispatch message
func (w *WorkerWithBuffer) dispatch() error {
	n := 0
	for {
		select {
		case msg := <-w.poolMSG:
			// check message send
			for {
				if n >= len(w.msg)-1 {
					n = 0
				}
				if len(w.msg[n]) < cap(w.msg[n]) {
					w.msg[n] <- msg
					n++
					break
				}
				n++
			}
		case <-w.signal:
			// clear message on worker
			return nil
		}
	}
	return nil
}

// worker this function we used for worker
func (w *WorkerWithBuffer) worker(workerNumber int, msg chan workerMessage, closeSignal chan struct{}) {
	for {
		select {
		case payload := <-msg:
			func() {
				defer w.recover(fmt.Sprintf("worker %d ", workerNumber))
				w.fn(payload.ctx, payload.msg)
			}()
		case <-closeSignal:
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
			log.Print(ErrorWorkerClose)
			return errors.New(ErrorWorkerClose)
		default:
			if len(w.poolMSG) < cap(w.poolMSG) {
				w.poolMSG <- workerMessage{
					ctx: ctx,
					msg: payload,
				}
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
		go func(fWG *sync.WaitGroup, payload workerMessage) {
			defer func() {
				fWG.Done()
				w.recover(fmt.Sprint("clean up pool message"))
			}()

			w.fn(payload.ctx, payload.msg)
		}(wg, <-w.poolMSG)
	}

	for n := 0; n < len(w.msg); n++ {
		for i := len(w.msg[n]); i > 0; i-- {
			wg.Add(1)
			go func(fWG *sync.WaitGroup, payload workerMessage) {
				defer func() {
					fWG.Done()
					w.recover(fmt.Sprintf("clean up message pool "))
				}()
				w.fn(payload.ctx, payload.msg)
			}(wg, <-w.msg[n])
		}
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
			w.worker(msgIndex, w.msg[msgIndex], w.signal)
		}(wg, n)
	}
	wg.Wait()
	// check clear Message
	w.cleanUpMessage()

	for n := 0; n < len(w.msg); n++ {
		close(w.msg[n])
	}

}

// Stop this for stop process
func (w *WorkerWithBuffer) Stop() {
	close(w.signal)
}

// recover this for recover
func (w *WorkerWithBuffer) recover(eventName string) {
	if r := recover(); r != nil {
		log.Printf("%s recovered err: %v stack_trace: %v \n", eventName, r, string(debug.Stack()))

	}
}
