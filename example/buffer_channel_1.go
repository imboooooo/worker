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
		MessageSize: 100,
		Worker:      100,
		FN: func(ctx context.Context, payload string) error {
			fmt.Println("process message ", payload)
			time.Sleep(12 * time.Second)
			f, _ := os.Create(fmt.Sprint("./temp/file_", payload))
			defer f.Close()
			return nil
		},
	},
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			err := worker.SendJob(context.Background(), fmt.Sprint("", i))
			if err == nil {
				fmt.Println("success send message ", i)
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
