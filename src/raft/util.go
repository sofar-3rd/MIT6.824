package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// heartBeatTimeout 100 ms
// electionTimeout 300ms - 500ms

func RandomizedElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	num := rand.Intn(4)*100 + 300
	return time.Duration(num) * time.Millisecond
}

func HeartbeatTimeout() time.Duration {
	return time.Duration(100) * time.Millisecond
}
