package utils

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"time"
)

func GetCurrentTimeStr() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}

func OutLog(str string) {
	fmt.Println(str)
}

func I32tob(val uint32) []byte {
	r := make([]byte, 4)
	for i := uint32(0); i < 4; i++ {
		r[i] = byte((val >> (8 * i)) & 0xff)
	}
	return r
}

func Btoi32(val []byte) uint32 {
	r := uint32(0)
	for i := uint32(0); i < 4; i++ {
		r |= uint32(val[i]) << (8 * i)
	}
	return r
}

func HexDecodeString(_str string) []byte {
	var str = _str
	if _str[0:2] == "0x" {
		str = _str[2:]
	}
	if len(str) == 0 {
		return []byte{}
	}

	if len(str)%2 == 1 {
		buf, _ := hex.DecodeString("0" + str)
		return buf
	}
	buf, _ := hex.DecodeString(str)
	return buf
}

func HexToBigInt(str string) *big.Int {
	buf := HexDecodeString(str)
	return new(big.Int).SetBytes(buf)
}

func HexToBytes(str string) []byte {
	buf := HexDecodeString(str)
	return buf
}

func HexToInt(str string) int {
	buf := HexDecodeString(str)
	return int(int32(binary.BigEndian.Uint32(buf)))
}

func HexToUint(str string) uint64 {
	buf := HexDecodeString(str)
	ret := make([]byte, 8)
	copy(ret[8-len(buf):], buf)
	return binary.BigEndian.Uint64(ret)
}
