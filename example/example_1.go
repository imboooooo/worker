package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"zingerone/worker"
)

// extend main
func main() {
	var wg sync.WaitGroup
	// Handle SIGINT and SIGTERM.
	ch := make(chan os.Signal)

	var worker = worker.NewWorker(100, func(payload string) error {
		fmt.Println("execute ", payload)
		return nil
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			worker.SendJob(fmt.Sprintf(" msg :%d", i))
		}
	}()

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

	wg.Wait()
}
