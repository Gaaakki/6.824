package raft

import "time"

type Entry struct {
	Index int
	Term  int
}

type RequestAppendLogArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type RequestAppendLogReply struct {
	Term    int
	Success bool
}

func (rf *Raft) RequestAppendLog(args *RequestAppendLogArgs, reply *RequestAppendLogReply) {
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		rf.updateTermAndStatus(args.Term)
	}
	rf.lastHeartbeatTime = time.Now().UnixNano()
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestAppendLog(server int, args *RequestAppendLogArgs, reply *RequestAppendLogReply) {
	ok := rf.peers[server].Call("Raft.RequestAppendLog", args, reply)
	if !ok {
		//DPrintf("this is NO.%d server, sendRequestAppendLog to NO.%d server failed\n", rf.me, server)
		return
	}
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.updateTermAndStatus(reply.Term)
	}
	rf.mu.Unlock()
}