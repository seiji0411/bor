package nodeipc

import (
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/nodeipc/constant"
	"github.com/ethereum/go-ethereum/nodeipc/message"
	"github.com/ethereum/go-ethereum/nodeipc/utils"
	ipc "github.com/james-barrow/golang-ipc"
	"sync"
	"time"
)

type BotClient struct {
	Submit    *message.Server
	Subscribe *message.Client
}

type Server struct {
	botClients map[string]*BotClient
	mutexBot   *sync.RWMutex
	serverMain *message.Server
	dataChan   chan []byte
	txChan     chan *types.Transaction
}

var instance *Server

func Shared() *Server {
	if instance != nil {
		return instance
	}
	instance = &Server{
		botClients: make(map[string]*BotClient),
		mutexBot:   new(sync.RWMutex),
		dataChan:   make(chan []byte, 1000),
	}
	return instance
}

func (s *Server) Run(txChan chan *types.Transaction) {
	log.Info("IpcServer Start")
	botMainMonitor := func(msg *ipc.Message) {
		switch message.MsgType(msg.MsgType) {
		case message.MsgTypeNewClient:
			go s.addBotClient(string(msg.Data))
		}
	}
	s.serverMain = message.NewServer(constant.BotMainIPC, botMainMonitor)

	s.txChan = txChan
	go s.serverMain.Run()
	go s.startServerSchedule()
	go s.RunSendLog()
}

func (s *Server) addBotClient(name string) {
	log.Info("IpcServer received new bot client", "client", name)
	subscribeSubmit := func(msg *ipc.Message) {
		switch message.MsgType(msg.MsgType) {
		case message.MsgTypeMessage:
			go s.submitTransaction(name, msg.Data)
		}
	}
	s.mutexBot.Lock()
	subscribeClient := message.NewClient(name, nil)
	submitName := name + "_submit"
	submitServer := message.NewServer(submitName, subscribeSubmit)
	s.botClients[name] = &BotClient{
		Submit:    submitServer,
		Subscribe: subscribeClient,
	}
	s.mutexBot.Unlock()

	go subscribeClient.Run()
	go submitServer.Run()
	time.Sleep(time.Second * 2)

	log.Info("IpcServer sending create submit to client", "client", name, "submit", submitName)
	err := subscribeClient.SendCreateSubmit(submitName)
	if err != nil {
		log.Info("IpcServer Failed to send submit create request", "client", name)
		return
	}
}

func (s *Server) submitTransaction(client string, txnData []byte) {
	log.Info("IpcServer received submit Request", "client", client, "txData", fmt.Sprintf("0x%x", txnData))
	// todo submit
	tx := new(types.Transaction)
	if err := tx.UnmarshalBinary(txnData); err != nil {
		log.Info("unmarshal transaction failed", "err", err.Error())
		return
	}
	s.txChan <- tx
}

func (s *Server) RunSendLog() {
	go func() {
		for {
			select {
			case data := <-s.dataChan:
				s.mutexBot.RLock()
				//log.Info("IpcServer BroadcastLog start", "clients", len(s.botClients), "len", len(data))
				for _, botClient := range s.botClients {
					_ = botClient.Subscribe.SendData(data)
				}
				//log.Info("IpcServer BroadcastLog end", "len", len(data))
				s.mutexBot.RUnlock()
			}
		}
	}()
}

func (s *Server) BroadcastBlockStart(number uint32, timestamp uint32) {
	bs, _ := json.Marshal(message.IPCBlock{
		BlockNumber:    number,
		BlockTimestamp: timestamp,
	})
	d, _ := json.Marshal(&message.IPCMessage{
		MessageType: message.MsgTypeBlockStart,
		MessageData: bs,
	})
	s.dataChan <- d
}

func (s *Server) BroadcastLog(logChan chan []*types.Log) {
	for {
		select {
		case logData := <-logChan:
			for _, data := range logData {
				bl, _ := data.MarshalJSON()
				d, _ := json.Marshal(&message.IPCMessage{
					MessageType: message.MsgTypeLog,
					MessageData: bl,
				})
				s.dataChan <- d
			}
		}
	}
}

func (s *Server) BroadcastBlockEnd(number uint32) {
	d, _ := json.Marshal(&message.IPCMessage{
		MessageType: message.MsgTypeBlockEnd,
		MessageData: utils.I32tob(number),
	})
	s.dataChan <- d
}

func (s *Server) BroadcastPendingTx(tx *types.Transaction) {
	ts, _ := tx.MarshalJSON()
	d, _ := json.Marshal(&message.IPCMessage{
		MessageType: message.MsgTypePendingTx,
		MessageData: ts,
	})
	s.dataChan <- d
}

func (s *Server) checkClientStatus() {
	nowTs := time.Now().UnixMilli()
	var invalidClients []string
	s.mutexBot.RLock()
	for name, botClient := range s.botClients {
		if nowTs > botClient.Subscribe.LastPingTs+10_000 {
			invalidClients = append(invalidClients, name)
		}
	}
	s.mutexBot.RUnlock()

	if len(invalidClients) > 0 {
		s.mutexBot.Lock()
		for _, name := range invalidClients {
			log.Info("IpcServer remove invalid client", "client", name)
			s.botClients[name].Submit.Close()
			s.botClients[name].Subscribe.Close()
			delete(s.botClients, name)
		}
		s.mutexBot.Unlock()
	}
}

func (s *Server) startServerSchedule() {
	ticker5Sec := time.NewTicker(5 * time.Second)
	go func() {
		for {
			select {
			case <-ticker5Sec.C:
				go s.checkClientStatus()
			}
		}
	}()
}
