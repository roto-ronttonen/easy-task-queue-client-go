package main

import (
	"log"

	easytaskqueueclientgo "github.com/roto-ronttonen/easy-task-queue-client-go"
)

func add() {
	a := 1 + 1
	log.Printf("%d", a)
}

func main() {
	workerClient := easytaskqueueclientgo.NewWorkerClient("localhost:1993", "add")

	err := workerClient.Start(add)

	if err != nil {
		log.Fatal(err.Error())
	}
}
