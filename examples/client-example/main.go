package main

import (
	"log"

	easytaskqueueclientgo "github.com/roto-ronttonen/easy-task-queue-client-go"
)

func main() {
	client := easytaskqueueclientgo.NewClient("localhost:1993")

	err := client.SendTask("add")

	if err != nil {
		log.Fatal(err.Error())
	}
}
