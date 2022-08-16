package shardctrler

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.RWMutex
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	stateMachine   ConfigStateMachine            // Config stateMachine
	lastOperations map[int64]OperationContext    // determine whether log is duplicated by recording the last commandId and response corresponding to the clientId
	notifyChans    map[int]chan *CommandResponse // notify client goroutine by applier goroutine to response
}

func (sc *ShardCtrler) Command(request *CommandRequest, response *CommandResponse) {
	defer DPrintf("{Node %v} processes CommandRequest %v with CommandResponse %v", sc.rf.Me(), request, response)
	// return result directly without raft layer's participation if request is duplicated
	sc.mu.RLock()
	if request.Op != OpQuery && sc.isDuplicateRequest(request.ClientId, request.CommandId) {
		lastResponse := sc.lastOperations[request.ClientId].LastResponse
		response.Config, response.Err = lastResponse.Config, lastResponse.Err
		sc.mu.RUnlock()
		return
	}
	sc.mu.RUnlock()
	// do not hold lock to improve throughput
	// when KVServer holds the lock to take snapshot, underlying raft can still commit raft logs
	index, _, isLeader := sc.rf.Start(Command{request})
	if !isLeader {
		response.Err = ErrWrongLeader
		return
	}
	sc.mu.Lock()
	ch := sc.getNotifyChan(index)
	sc.mu.Unlock()
	select {
	case result := <-ch:
		response.Config, response.Err = result.Config, result.Err
	case <-time.After(ExecuteTimeout):
		response.Err = ErrTimeout
	}
	// release notifyChan to reduce memory footprint
	// asynchronously to improve throughput, here is no need to block client request
	go func() {
		sc.mu.Lock()
		sc.removeOutdatedNotifyChan(index)
		sc.mu.Unlock()
	}()
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// each RPC imply that the client has seen the reply for its previous RPC
// therefore we only need to determine whether the latest commandId of a clientId meets the criteria
func (sc *ShardCtrler) isDuplicateRequest(clientId int64, requestId int64) bool {
	operationContext, ok := sc.lastOperations[clientId]
	return ok && requestId <= operationContext.MaxAppliedCommandId
}

func (sc *ShardCtrler) getNotifyChan(index int) chan *CommandResponse {
	if _, ok := sc.notifyChans[index]; !ok {
		sc.notifyChans[index] = make(chan *CommandResponse, 1)
	}
	return sc.notifyChans[index]
}

func (sc *ShardCtrler) removeOutdatedNotifyChan(index int) {
	delete(sc.notifyChans, index)
}

func (sc *ShardCtrler) applyLogToStateMachine(command Command) *CommandResponse {
	var config Config
	var err Err
	switch command.Op {
	case OpJoin:
		err = sc.stateMachine.Join(command.Servers)
	case OpLeave:
		err = sc.stateMachine.Leave(command.GIDs)
	case OpMove:
		err = sc.stateMachine.Move(command.Shard, command.GID)
	case OpQuery:
		config, err = sc.stateMachine.Query(command.Num)
	}
	return &CommandResponse{err, config}
}

// A dedicated applier goroutine to apply committed entries to stateMachine
func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		message := <-sc.applyCh
		DPrintf("{Node %v} tries to apply message %v", sc.rf.Me(), message)
		if message.CommandValid {
			sc.mu.Lock()
			var response *CommandResponse
			// type conversion
			command := message.Command.(Command)
			if command.Op != OpQuery && sc.isDuplicateRequest(command.ClientId, command.CommandId) {
				DPrintf("{Node %v} doesn't apply duplicated message %v to stateMachine because maxAppliedCommandId is %v for client %v", sc.rf.Me(), message, sc.lastOperations[command.ClientId], command.ClientId)
				response = sc.lastOperations[command.ClientId].LastResponse
			} else {
				response = sc.applyLogToStateMachine(command)
				if command.Op != OpQuery {
					sc.lastOperations[command.ClientId] = OperationContext{command.CommandId, response}
				}
			}

			// only notify related channel for currentTerm's log when node is leader
			if currentTerm, isLeader := sc.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
				ch := sc.getNotifyChan(message.CommandIndex)
				ch <- response
			}

			sc.mu.Unlock()
		} else {
			panic(fmt.Sprintf("Unexpected message: %v", message))
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	applyCh := make(chan raft.ApplyMsg)

	sc := &ShardCtrler{
		rf:             raft.Make(servers, me, persister, applyCh),
		applyCh:        applyCh,
		stateMachine:   NewMemoryConfigStateMachine(),
		lastOperations: make(map[int64]OperationContext),
		notifyChans:    make(map[int]chan *CommandResponse),
	}
	// start applier goroutine to apply committed logs to stateMachine
	go sc.applier()

	DPrintf("{ShardCtrler %v} has started", sc.rf.Me())
	return sc
}
