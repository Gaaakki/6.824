package raft

import (
	"math/rand"
	"time"
)

const CHECK_TIMEOUT_INTERVAL = 50

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		reply.VoteGranted = true
		rf.status = FOLLOWER
		rf.voteNum = 0
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.lastHeartbeatTime = time.Now().UnixNano()
	} else if args.Term < rf.currentTerm || (rf.votedFor != - 1 && rf.votedFor != args.CandidateId) {
		reply.VoteGranted = false
	} else {
		reply.VoteGranted = true
		rf.lastHeartbeatTime = time.Now().UnixNano()
		rf.votedFor = args.CandidateId
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.updateTermAndStatus(reply.Term)
	}
	if reply.VoteGranted && rf.status == CANDIDATE {
		rf.voteNum++
		DPrintf("this is NO.%d server, voteNum = %d\n", rf.me, rf.voteNum)
		if rf.voteNum > len(rf.peers)/2 {
			rf.status = LEADER
			rf.lastHeartbeatTime = time.Now().UnixNano()
			go rf.sendHeartBeat()
		}
	}
	rf.mu.Unlock()
}

// 开始选举，选举时发送给各节点的参数是相同的
func (rf *Raft) startElection() {
	rf.mu.Lock()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			reply := &RequestVoteReply{}
			go rf.sendRequestVote(i, args, reply)
		}
	}
}

// 每个50ms检查一次是否发生了选举超时或心跳超时
func (rf *Raft) electionTicker() {
	rand.Seed(time.Now().UnixNano())
	for {
		if rf.killed() {
			return
		}
		currentTime := time.Now().UnixNano()
		electionTimeout := rand.Int63n(250*1e6) + 250*1e6
		rf.mu.Lock()
		timeInterval := currentTime - rf.lastHeartbeatTime
		if timeInterval > electionTimeout && (rf.status == FOLLOWER || rf.status == CANDIDATE) {
			rf.currentTerm++
			rf.status = CANDIDATE
			rf.votedFor = rf.me
			rf.voteNum = 1
			rf.lastHeartbeatTime = currentTime
			DPrintf("this is NO.%d server, start election,term = %d\n", rf.me, rf.currentTerm)
			go rf.startElection()
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * CHECK_TIMEOUT_INTERVAL)
	}
}
