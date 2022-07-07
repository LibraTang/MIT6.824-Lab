package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term     int
	Success  bool
	Conflict bool
	// fast Backup优化需要这3个字段
	XTerm  int // Follower中与Leader冲突的Log对应的任期号; 如果Follower在对应位置没有Log，那么这里会返回 -1
	XIndex int // 这个是Follower中，对应任期号为XTerm的第一条Log条目的槽位号
	XLen   int // 如果Follower在对应位置没有Log，那么XTerm会返回-1，XLen表示Follower当前Entry长度
}

//
// Leader发送带log的Entry或者心跳包
//
func (rf *Raft) appendEntries(heartBeat bool) {
	lastLog := rf.log.lastLog()
	for peer := range rf.peers {
		if peer == rf.me {
			// 重置选举计时器
			rf.resetElectionTimer()
			continue
		}
		// rule 3 for leader
		if lastLog.Index >= rf.nextIndex[peer] || heartBeat {
			nextIndex := rf.nextIndex[peer]
			if nextIndex <= 0 {
				nextIndex = 1
			}
			// 这应该是处理log不一致状况？减小nextIndex
			if lastLog.Index+1 < nextIndex {
				nextIndex = lastLog.Index
			}
			prevLog := rf.log.at(nextIndex - 1)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLog.Index,
				PrevLogTerm:  prevLog.Term,
				Entries:      make([]Entry, lastLog.Index-nextIndex+1),
				LeaderCommit: rf.commitIndex,
			}
			copy(args.Entries, rf.log.slice(nextIndex))
			// 并行的给所有server发送消息
			go rf.leaderSendEntries(peer, &args)
		}
	}
}

//
// Leader并行的给所有server发送消息
//
func (rf *Raft) leaderSendEntries(serverId int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(serverId, args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		return
	}
	if args.Term == rf.currentTerm {
		// rule 3.1 for leader
		if reply.Success {
			matchIndex := args.PrevLogIndex + len(args.Entries)
			nextIndex := matchIndex + 1
			rf.nextIndex[serverId] = max(rf.nextIndex[serverId], nextIndex)
			rf.matchIndex[serverId] = max(rf.matchIndex[serverId], matchIndex)
			DPrintf("[%v]: %v append success, nextIndex %v, matchIndex %v\n", rf.me, serverId, rf.nextIndex[serverId], rf.matchIndex[serverId])
		} else if reply.Conflict {
			DPrintf("[%v]: Conflict from %v %#v\n", rf.me, serverId, reply)
			if reply.XTerm == -1 {
				rf.nextIndex[serverId] = reply.XLen
			} else {
				lastLogInXTerm := rf.findLastLogInTerm(reply.XTerm)
				if lastLogInXTerm > 0 {
					rf.nextIndex[serverId] = lastLogInXTerm
				} else {
					rf.nextIndex[serverId] = reply.XIndex
				}
			}
			DPrintf("[%v]: Leader nextIndex[%v] %v\n", rf.me, serverId, rf.nextIndex[serverId])
		} else if rf.nextIndex[serverId] > 1 {
			rf.nextIndex[serverId]--
		}
		rf.leaderCommitRule()
	}

}

//
// 找到第x term的最后一个log
//
func (rf *Raft) findLastLogInTerm(x int) int {
	for i := rf.log.lastLog().Index; i > 0; i-- {
		term := rf.log.at(i).Term
		if term == x {
			return i
		} else if term < x {
			break
		}
	}
	return -1
}

//
// Leader提交log的规则
//
func (rf *Raft) leaderCommitRule() {
	if rf.state != Leader {
		return
	}

	// rule 4 for leader
	for n := rf.commitIndex + 1; n <= rf.log.lastLog().Index; n++ {
		if rf.log.at(n).Term != rf.currentTerm {
			continue
		}
		counter := 1
		for serverId := 0; serverId < len(rf.peers); serverId++ {
			if serverId != rf.me && rf.matchIndex[serverId] >= n {
				counter++
			}
			if counter > len(rf.peers)/2 {
				rf.commitIndex = n
				DPrintf("[%v]: Leader try to apply index %v\n", rf.me, rf.commitIndex)
				rf.apply()
				break
			}
		}
	}
}

//
// server收到entries rpc的处理
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d]: (Term %d) Follwer received [%v] AppendEntries %v, prevIndex %v, prevTerm %v\n", rf.me, rf.currentTerm, args.LeaderId, args.Entries, args.PrevLogIndex, args.PrevLogTerm)
	// rules for servers
	// rule 2 for all servers
	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
		return
	}

	// rule 1 for AppendEntries RPC
	if args.Term < rf.currentTerm {
		return
	}
	rf.resetElectionTimer()

	// rule 3 for candidate
	if rf.state == Candidate {
		rf.state = Follower
	}

	// rule 2 for AppendEntries RPC
	if rf.log.lastLog().Index < args.PrevLogIndex {
		reply.Conflict = true
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.log.len()
		DPrintf("[%v]: Conflict XTerm %v, XIndex %v, XLen %v\n", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}
	if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Conflict = true
		xTerm := rf.log.at(args.PrevLogIndex).Term
		for xIndex := args.PrevLogIndex; xIndex > 0; xIndex-- {
			if rf.log.at(xIndex-1).Term != xTerm {
				reply.XIndex = xIndex
				break
			}
		}
		reply.XTerm = xTerm
		reply.XLen = rf.log.len()
		DPrintf("[%v]: Conflict XTerm %v, XIndex %v, XLen %v\n", rf.me, reply.XTerm, reply.XIndex, reply.XLen)
		return
	}

	// 复制log到Follower
	for idx, entry := range args.Entries {
		// rule 3 for AppendEntries RPC
		if entry.Index <= rf.log.lastLog().Index && rf.log.at(entry.Index).Term != entry.Term {
			rf.log.truncate(entry.Index)
			rf.persist()
		}
		// rule 4 for AppendEntries RPC
		if entry.Index > rf.log.lastLog().Index {
			rf.log.append(args.Entries[idx:]...)
			DPrintf("[%d]: Follower append [%v]\n", rf.me, args.Entries[idx:])
			rf.persist()
			break
		}
	}

	// rule 5 for AppendEntries RPC
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log.lastLog().Index)
		rf.apply()
	}
	reply.Success = true
}

//
// RPC调用AppendEntries
//
func (rf *Raft) sendAppendEntries(serverId int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[serverId].Call("Raft.AppendEntries", args, reply)
	return ok
}
