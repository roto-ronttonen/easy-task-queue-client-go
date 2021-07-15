package easytaskqueueclientgo

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type WorkerClient struct {
	Connection *EasyTaskQueueConnection
	TaskType   string
	ListenPort string
	Lock       *sync.Mutex
}

func NewWorkerClient(address string, taskType string) *WorkerClient {
	conn := NewConnection(address)
	p := os.Getenv("PORT")
	if len(p) == 0 {
		p = "1994"
	}
	return &WorkerClient{
		Connection: conn,
		TaskType:   taskType,
		ListenPort: p,
	}
}

func HeartbeatRoutine(workerClient *WorkerClient) {
	log.Print("Started heartbeat routine")
	for {
		time.Sleep(30 * time.Second)
		err := workerClient.RunHeartbeat()
		if err != nil {
			workerClient.Lock.Lock()
			workerClient.Connection.ConnectedStatus = false
			workerClient.Lock.Unlock()
			err := Retry(100, 10, workerClient.TryReconnect)
			if err != nil {
				log.Fatalf("Failed to connect to task queue after 100 attempts, with error: %s", err)
			}
		}

	}

}

func ListenForTaskStart(workerClient *WorkerClient, task func()) error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%s", workerClient.ListenPort))
	if err != nil {
		return err
	}
	// Setup heartbeat routine
	go HeartbeatRoutine(workerClient)
	log.Printf("Started listening for requests")
	for {
		conn, err := ln.Accept()
		if err != nil {
			// Todo add something that makes sense here
			log.Print("Unable to handle request")
			continue
		}

		go HandleConnection(conn, workerClient, task)

	}
}

func HandleConnection(conn net.Conn, workerClient *WorkerClient, task func()) {
	buf := make([]byte, 1024)

	workerClient.Lock.Lock()
	if !workerClient.Connection.ConnectedStatus {
		log.Print("Tried connection without connection ready")
		conn.Write([]byte("Tried connection without connection ready"))
		conn.Close()
		workerClient.Lock.Unlock()
		return
	}

	workerClient.Lock.Unlock()

	_, err := conn.Read(buf)
	if err != nil {
		log.Printf("Error reading: %s", err.Error())
		conn.Write([]byte("Error reading message"))
		conn.Close()
		return
	}

	str := string(bytes.Trim(buf, "\x00"))

	fmt.Printf("Received command: %s", str)

	message := strings.Split(str, ":")

	if len(message) != 2 {
		log.Print("Invalid message")
		conn.Write([]byte("Invalid message"))
		conn.Close()
		return
	}

	if message[0] != "worker" || message[1] != "start" {
		log.Print("Invalid message")
		conn.Write([]byte("Invalid message"))
		conn.Close()
		return
	}

	conn.Write([]byte("worker:ack"))
	conn.Close()
	task()
	workerClient.SendReady()
}

func (workerClient *WorkerClient) Start(task func()) error {
	// Create connection
	err := workerClient.Connect()
	if err != nil {
		return err
	}

	// Listen for task start
	err = ListenForTaskStart(workerClient, task)
	return err

}

func (workerClient *WorkerClient) Connect() error {
	reply, err := SendTcp(workerClient.Connection.EasyTaskQueueAddress, fmt.Sprintf("worker:join:%s:%s", workerClient.TaskType, workerClient.ListenPort))

	if err != nil {
		return err
	}

	if reply != "success" {
		log.Printf("%v, %s, %d, %d", reply != "success", reply, len(reply), len("success"))
		return errors.New(reply)
	}
	workerClient.Lock.Lock()
	workerClient.Connection.ConnectedStatus = true
	workerClient.Lock.Unlock()
	return nil
}

func (workerClient *WorkerClient) TryReconnect() error {
	err := workerClient.Connect()
	return err
}

func (workerClient *WorkerClient) RunHeartbeat() error {
	reply, err := SendTcp(workerClient.Connection.EasyTaskQueueAddress, "worker:heartbeat")
	workerClient.Lock.Lock()
	defer workerClient.Lock.Unlock()
	if err != nil {
		workerClient.Connection.ConnectedStatus = false
		return err
	}

	if reply != "success" {
		workerClient.Connection.ConnectedStatus = false
		return errors.New(reply)
	}

	return nil
}

func (workerClient *WorkerClient) Disconnect() error {
	reply, err := SendTcp(workerClient.Connection.EasyTaskQueueAddress, "worker:disconnect")
	workerClient.Lock.Lock()
	defer workerClient.Lock.Unlock()
	if err != nil {
		return err
	}

	if reply != "success" {
		return errors.New(reply)
	}

	workerClient.Connection.ConnectedStatus = false

	return nil
}

func (workerClient *WorkerClient) SendReady() error {
	reply, err := SendTcp(workerClient.Connection.EasyTaskQueueAddress, "worker:ready")

	if err != nil {
		return err
	}

	if reply != "success" {
		return errors.New(reply)
	}

	return nil
}
