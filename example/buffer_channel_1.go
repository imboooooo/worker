package main

import (
	"context"
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

	var worker = worker.NewWorkerWithBuffer(worker.ConfigWorkerWithBuffer{
		MessageSize: 50,
		Worker:      50,
		FN: func(ctx context.Context, payload string) error {
			f, _ := os.Create(fmt.Sprint("./temp/file_", payload))
			defer f.Close()
			return nil
		},
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			err := worker.SendJob(context.Background(), fmt.Sprint(i))
			if err == nil {
				fmt.Println("message success send ", i)
			}
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
