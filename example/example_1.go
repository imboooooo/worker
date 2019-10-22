package example

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"worker"
)

// extend main
func main() {
	var wg sync.WaitGroup
	// Handle SIGINT and SIGTERM.
	ch := make(chan os.Signal)

	var worker = worker.NewWorker(100, func(payload string) error {
		fmt.Println(payload)
		return nil
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		worker.Start()
		fmt.Println("close run worker")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		log.Println(<-ch)
		worker.Stop()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			worker.SendJob(fmt.Sprintf(" msg :%d", i))
		}
	}()

	wg.Wait()
}
