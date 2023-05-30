package utils

import (
	"fmt"
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
