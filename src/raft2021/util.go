package raft2021

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// Debugging
const Debug = false

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandTerm  int
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (msg ApplyMsg) String() string {
	if msg.CommandValid {
		return fmt.Sprintf("{Command:%v,CommandTerm:%v,CommandIndex:%v}", msg.Command, msg.CommandTerm, msg.CommandIndex)
	} else if msg.SnapshotValid {
		return fmt.Sprintf("{Snapshot:%v,SnapshotTerm:%v,SnapshotIndex:%v}", msg.Snapshot, msg.SnapshotTerm, msg.SnapshotIndex)
	} else {
		panic(fmt.Sprintf("unexpected ApplyMsg{CommandValid:%v,CommandTerm:%v,CommandIndex:%v,SnapshotValid:%v,SnapshotTerm:%v,SnapshotIndex:%v}", msg.CommandValid, msg.CommandTerm, msg.CommandIndex, msg.SnapshotValid, msg.SnapshotTerm, msg.SnapshotIndex))
	}
}

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

func (entry Entry) String() string {
	return fmt.Sprintf("{Index:%v,Term:%v}", entry.Index, entry.Term)
}

type NodeState uint8

const (
	StateFollower NodeState = iota
	StateCandidate
	StateLeader
)

func (state NodeState) String() string {
	switch state {
	case StateFollower:
		return "Follower"
	case StateCandidate:
		return "Candidate"
	case StateLeader:
		return "Leader"
	}
	panic(fmt.Sprintf("unexpected NodeState %d", state))
}

type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rand.Intn(n)
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// the duration of timeout cannot too long or too short
const (
	HeartbeatTimeout = 125
	ElectionTimeout  = 1000
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func RandomizedElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeout+globalRand.Intn(ElectionTimeout)) * time.Millisecond
}

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(HeartbeatTimeout) * time.Millisecond
}

func shrinkEntriesArray(entries []Entry) []Entry {
	const lenMultiple = 2
	if len(entries)*lenMultiple < cap(entries) {
		newEntries := make([]Entry, len(entries))
		copy(newEntries, entries)
		return newEntries
	}
	return entries
}
