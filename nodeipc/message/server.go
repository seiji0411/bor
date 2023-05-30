package message

import (
	"fmt"
	ipc "github.com/james-barrow/golang-ipc"
	"time"
)

type Server struct {
	name             string
	server           *ipc.Server
	LastPingTs       int64
	cbMessageProcess CbMessage
}

func NewServer(name string, cb CbMessage) *Server {
	var ipcName = name
	if len(ipcName) == 0 {
		ipcName = generatePipeName()
	}
	fmt.Println("Create server " + ipcName)
	cc, err := ipc.StartServer(ipcName, nil)
	if err != nil {
		fmt.Println("Failed to create server Error: " + err.Error())
		return nil
	}
	return &Server{
		name:             ipcName,
		server:           cc,
		LastPingTs:       time.Now().UnixMilli(),
		cbMessageProcess: cb,
	}
}

func (c *Server) GetPipeName() string {
	return c.name
}

func (c *Server) Run() {
	for {
		m, err := c.server.Read()
		if err != nil {
			fmt.Println("Server Run" + c.name + err.Error())
			break
		}

		if m.MsgType > 0 {
			c.doProcess(m)
		}
		//}
	}
}

func (c *Server) doProcess(msg *ipc.Message) {
	switch MsgType(msg.MsgType) {
	case MsgTypePing:
		c.doPing()
	}
	if c.cbMessageProcess != nil {
		go c.cbMessageProcess(msg)
	}
}

func (c *Server) doPing() {
	c.LastPingTs = time.Now().UnixMilli()
}

func (c *Server) SendPing() error {
	err := c.server.Write(int(MsgTypePing), []byte{1})
	return err
}

func (c *Server) SendData(data []byte) error {
	err := c.server.Write(int(MsgTypeMessage), data)
	return err
}

func (c *Server) Close() {
	c.server.Close()
}
