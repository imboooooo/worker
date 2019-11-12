package worker

import (
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
	worker.msg = make(chan string,numberWorker)

	return worker

}

// WorkerWithBuffer this struct for worker
type WorkerWithBuffer struct {
	signal    chan struct{}
	msg    chan string
	muMSG  sync.Mutex
	fn     func(string) error
}


// dispatch this function we used for dispatch message
func (w *WorkerWithBuffer) dispatch() error {
	var err error
	for {
		select {
		case <-w.signal:
			// clear message
			w.worker()

			log.Println("worker close signal")
			return nil
		default:
			w.worker()
		}
		time.Sleep(1 * time.Second)

	}
	return err
}

// worker this function we used for worker
func (w *WorkerWithBuffer) worker()  {
	wg:=sync.WaitGroup{}
	n:=len(w.msg)
	for i:=1;i<=n;i++ {
		wg.Add(1)
		go func(payload string,fWG *sync.WaitGroup) {
			defer fWG.Done()
			w.fn(payload)
		}(<-w.msg,&wg)
	}
	wg.Wait()
}

//SendJob this for send job
func (w *WorkerWithBuffer) SendJob(payload string) {

	for {
		select {
		case <-w.signal:
			return
		default:
			if len(w.msg)< cap(w.msg) {
				w.msg<-payload
				return
			}

		}

		time.Sleep(1 * time.Second)
	}
}

// Start this for wait process
func (w *WorkerWithBuffer) Start() {
	w.dispatch()
}

// Stop this for stop process
func (w *WorkerWithBuffer) Stop() {
	close(w.signal)
}
