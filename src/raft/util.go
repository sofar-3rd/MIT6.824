package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// heartBeatTimeout 200 ms
// electionTimeout 300ms - 500ms

const (
	heartBeatTimeout     int = 200
	minElectionTimeout   int = 600
	rangeElectionTimeout int = 2
)

func RandomizedElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	num := rand.Intn(rangeElectionTimeout+1)*heartBeatTimeout + minElectionTimeout
	return time.Duration(num) * time.Millisecond
}

func HeartbeatTimeout() time.Duration {
	return time.Duration(heartBeatTimeout) * time.Millisecond
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func Max(x, y int) int {
	if x < y {
		return y
	}
	return x
}
