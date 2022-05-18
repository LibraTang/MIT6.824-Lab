package raft

import "sync"

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// Server收到RequestVote RPC之后的处理
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Lock()

	// 若请求参数的Term比服务器当前的Term大，更新当前Term
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
	}

	// 若请求参数的Term小于当前服务器的Term，视为请求无效
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	myLastLog := rf.log.lastLog()
	// 判断请求参数是否过时
	upToDate := args.LastLogTerm > myLastLog.Term || (args.LastLogIndex == myLastLog.Term && args.LastLogIndex >= myLastLog.Index)
	// 同意投票
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.resetElectionTimer()
		DPrintf("[%v]: Term %v vote %v", rf.me, rf.currentTerm, rf.votedFor)
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// Candidate发送投票请求
//
func (rf *Raft) candidateRequestVote(serverId int, args *RequestVoteArgs, voteCounter *int, becomeLeader *sync.Once) {
	DPrintf("[%d]: Term %v send vote request to %d\n", rf.me, args.Term, serverId)
	reply := RequestVoteReply{}
	// 向指定的server发送投票请求
	ok := rf.sendRequestVote(serverId, args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > args.Term {
		// Candidate的Term太旧，更新Term
		DPrintf("[%d]: Server %d in new term, update term, end\n", rf.me, serverId)
		rf.setNewTerm(reply.Term)
		return
	}
	if reply.Term < args.Term {
		// reply的Term过时，失效
		DPrintf("[%d]: Server %d term %d is not up-to-date, end\n", rf.me, serverId, reply.Term)
		return
	}
	if !reply.VoteGranted {
		// Peer没有给me投票
		DPrintf("[%d]: Server %d did not vote to me, end\n", rf.me, serverId)
		return
	}
	DPrintf("[%d]: Server %d voted for me\n", rf.me, serverId)

	// 投票数+1
	*voteCounter++

	// 得到大多数选票，成为Leader
	if *voteCounter > len(rf.peers)/2 && rf.currentTerm == args.Term && rf.state == Candidate {
		DPrintf("[%d]: Get a majority of vote, end\n", rf.me)
		// 使用sync.Once保证转换为Leader只发生一次
		becomeLeader.Do(func() {
			DPrintf("[%d]: Current term %d end\n", rf.me, rf.currentTerm)
			rf.state = Leader
			lastLogIndex := rf.log.lastLog().Index
			// 选举出leader后，初始化nextIndex[]和matchIndex[]
			for i, _ := range rf.peers {
				rf.nextIndex[i] = lastLogIndex + 1
				rf.matchIndex[i] = 0
			}
			DPrintf("[%d]: Leader nextIndex: %#v", rf.me, rf.nextIndex)
			// 马上发送一次心跳包
			rf.appendEntries(true)
		})
	}
}
