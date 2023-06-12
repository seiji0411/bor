package message

import (
	"github.com/ethereum/go-ethereum/core/types"
	ipc "github.com/james-barrow/golang-ipc"
)

type MsgType int

const PingIntervalTime = 5 // seconds
const (
	MsgTypeNone         MsgType = iota
	MsgTypeNewClient    MsgType = iota
	MsgTypeMessage      MsgType = iota
	MsgTypePing         MsgType = iota
	MsgTypeCreateSubmit MsgType = iota
	MsgTypeBlockStart   MsgType = iota
	MsgTypeLog          MsgType = iota
	MsgTypeBlockEnd     MsgType = iota
	MsgTypePendingTx    MsgType = iota
)

type CbMessage func(msg *ipc.Message)

type IPCMessage struct {
	MessageType MsgType `json:"messageType"`
	MessageData []byte  `json:"messageData"`
}

type IPCBlock struct {
	BlockNumber    uint32 `json:"blockNumber"`
	BlockTimestamp uint32 `json:"blockTimestamp"`
}

type PendingTx struct {
	Tx   *types.Message
	Logs []*types.Log
}
