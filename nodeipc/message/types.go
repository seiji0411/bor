package message

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	ipc "github.com/james-barrow/golang-ipc"
	"math/big"
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
	TxHash    *common.Hash    `json:"txHash"`
	Type      uint8           `json:"Type"`
	To        *common.Address `json:"to"`
	From      common.Address  `json:"from"`
	Nonce     uint64          `json:"nonce"`
	Amount    *big.Int        `json:"amount"`
	GasLimit  uint64          `json:"gasLimit"`
	GasPrice  *big.Int        `json:"gasPrice"`
	GasFeeCap *big.Int        `json:"gasFeeCap"`
	GasTipCap *big.Int        `json:"gasTipCap"`
	Data      []byte          `json:"data"`
	Logs      []*types.Log    `json:"logs"`
}
