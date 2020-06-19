package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"zingerone/worker"
)

// extend main
func main() {
	var wg sync.WaitGroup
	// Handle SIGINT and SIGTERM.
	ch := make(chan os.Signal)

	var worker = worker.NewWorkerWithBuffer(1000, func(payload string) error {
		//time.Sleep(1 * time.Second)
		f, _ := os.Create(fmt.Sprint("./temp/file_", payload))
		defer f.Close()
		return nil
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			worker.SendJob(fmt.Sprint("", i))
		}
	}()
	time.Sleep(time.Second * 1)

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
