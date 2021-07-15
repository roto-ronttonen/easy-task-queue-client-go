package easytaskqueueclientgo

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"time"
)

type EasyTaskQueueConnection struct {
	EasyTaskQueueAddress string
	ConnectedStatus      bool
}

func NewConnection(address string) *EasyTaskQueueConnection {
	conn := EasyTaskQueueConnection{
		ConnectedStatus:      false,
		EasyTaskQueueAddress: address,
	}
	return &conn
}

func SendTcp(address string, message string) (string, error) {
	tcpConn, err := net.Dial("tcp", address)
	if err != nil {
		return "", err
	}

	log.Printf("Sent message: %s", message)
	_, err = tcpConn.Write([]byte(message))
	if err != nil {
		return "", err
	}
	reply := make([]byte, 64)

	_, err = tcpConn.Read(reply)

	if err != nil {
		return "", err
	}

	r := string(bytes.Trim(reply, "\x00"))

	log.Printf("Received: %s", r)

	return r, nil

}

func Retry(attempts int, sleep time.Duration, f func() error) (err error) {
	for i := 0; ; i++ {
		err = f()
		if err == nil {
			return
		}

		if i >= (attempts - 1) {
			break
		}

		time.Sleep(sleep)

		log.Println("retrying after error:", err)
	}
	return fmt.Errorf("after %d attempts, last error: %s", attempts, err)
}
