package easytaskqueueclientgo

import (
	"errors"
	"fmt"
)

type Client struct {
	Connection *EasyTaskQueueConnection
}

func NewClient(address string) *Client {
	connection := NewConnection(address)
	return &Client{
		Connection: connection,
	}
}

func (client *Client) SendTask(taskType string) error {
	message, err := SendTcp(client.Connection.EasyTaskQueueAddress, fmt.Sprintf("client:task:%s", taskType))
	if err != nil {
		return err
	}

	if message != "success" {
		return errors.New(message)
	}

	return nil
}

func (client *Client) SendTaskWithData(taskType string, data string) error {
	message, err := SendTcp(client.Connection.EasyTaskQueueAddress, fmt.Sprintf("client:task:%s:%s", taskType, data))
	if err != nil {
		return err
	}

	if message != "success" {
		return errors.New(message)
	}

	return nil
}
