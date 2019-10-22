package worker

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// message this for message worker
type message struct {
	payload   string
	isProcess bool
}

// Config this for config worker
type Config struct {
}

// NewWorker this for new worker
func NewWorker(numberWorker int, fn func(string) error) *Worker {
	var (
		worker = new(Worker)
		wg     sync.WaitGroup
	)
	worker.signal = make(chan struct{})
	worker.msg = make([]message, numberWorker)
	worker.fn = fn

	for i := 0; i < numberWorker; i++ {
		wg.Add(1)
		go worker.execute(&wg, i)

	}
	worker.wg = &wg
	return worker

}

// Worker this struct for worker
type Worker struct {
	signal chan struct{}
	msg    []message
	muMSG  sync.Mutex
	fn     func(string) error
	wg     *sync.WaitGroup
}

// setMessage this internal message to set message
func (w *Worker) setMessage(poss int, msg message) {
	w.muMSG.Lock()
	w.msg[poss] = msg
	w.muMSG.Unlock()
}

// message this function we used for get message
func (w *Worker) message(poss int) message {
	var msg message
	w.muMSG.Lock()
	msg = w.msg[poss]
	w.muMSG.Unlock()
	return msg
}

// execute this function we used for execute worker
func (w *Worker) execute(fwg *sync.WaitGroup, poss int) error {
	defer fwg.Done()

	var err error
	for {
		select {
		case <-w.signal:
			log.Println("close signal")
			return nil
		default:
			if msg := w.message(poss); msg.isProcess == false && msg.payload != "" {
				msg.isProcess = true
				w.setMessage(poss, msg)
				fmt.Printf("worker %d message : %s \n", poss, msg.payload)
				w.fn(msg.payload)
				w.setMessage(poss, message{})
			}

		}
		time.Sleep(1 * time.Second)

	}
	return err
}

//SendJob this for send job
func (w *Worker) SendJob(payload string) {
	size := len(w.msg)
	evenOdd := 0
	if time.Now().Unix()%2 == 0 {
		evenOdd = size - 1
	}
	for {
		for i := 0; i < size; i++ {
			select {
			case <-w.signal:
				fmt.Println("close send to queue")
				return
			default:
				j := i
				if evenOdd > 0 {
					j = evenOdd - i
				}
				if msg := w.message(j); msg.isProcess == false && msg.payload == "" {
					msg.payload = payload
					w.setMessage(j, msg)
					return
				}
			}

		}
		time.Sleep(1 * time.Second)
	}
}

// Start this for wait process
func (w *Worker) Start() {
	w.wg.Wait()
}

// Stop this for stop process
func (w *Worker) Stop() {
	close(w.signal)
}
