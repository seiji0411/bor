package message

import (
	"github.com/ethereum/go-ethereum/log"
	ipc "github.com/james-barrow/golang-ipc"
	"time"
)

type Client struct {
	name             string
	client           *ipc.Client
	LastPingTs       int64
	cbMessageProcess CbMessage
}

func NewClient(name string, cb CbMessage) *Client {
	log.Info("Create Client ", "Name", name)
	cc, err := ipc.StartClient(name, nil)
	if err != nil {
		log.Info("Failed to create client Error: ", "Error", err.Error())
		return nil
	}
	return &Client{
		name:             name,
		client:           cc,
		LastPingTs:       time.Now().UnixMilli(),
		cbMessageProcess: cb,
	}
}

func (c *Client) Run() {
	for {
		m, err := c.client.Read()
		if err != nil {
			log.Info("Server Run Error", "Client", c.name, "Error", err.Error())
			break
		}

		if m.MsgType > 0 {
			c.doProcess(m)
		}
	}
}

func (c *Client) doProcess(msg *ipc.Message) {
	switch MsgType(msg.MsgType) {
	case MsgTypePing:
		c.doPing()
	}
	if c.cbMessageProcess != nil {
		go c.cbMessageProcess(msg)
	}
}

func (c *Client) doPing() {
	c.LastPingTs = time.Now().UnixMilli()
}

func (c *Client) SendData(data []byte) error {
	err := c.client.Write(int(MsgTypeMessage), data)
	return err
}

func (c *Client) SendAddNewRequest(name string) error {
	err := c.client.Write(int(MsgTypeNewClient), []byte(name))
	return err
}

func (c *Client) SendCreateSubmit(submitPipeName string) error {
	err := c.client.Write(int(MsgTypeCreateSubmit), []byte(submitPipeName))
	return err
}

func (c *Client) Close() {
	c.client.Close()
}
