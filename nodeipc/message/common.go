package message

import (
	"fmt"
	"math/rand"
	"time"
)

func generatePipeName() string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	ch := r.Int63n(1000000)
	return fmt.Sprintf("bot%d_%d", time.Now().UnixMilli(), ch)
}
