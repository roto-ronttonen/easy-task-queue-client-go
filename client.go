package easytaskqueueclientgo

import "errors"

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
	message, err := SendTcp(client.Connection.EasyTaskQueueAddress, taskType)
	if err != nil {
		return err
	}

	if message != "success" {
		return errors.New(message)
	}

	return nil
}
