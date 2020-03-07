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
	if args.Term > rf.currentTerm {
		rf.updateTermAndStatus(args.Term)
	}

	reply.Term = rf.currentTerm
	localLastLogTerm := rf.log[len(rf.log) - 1].Term
	if args.Term < rf.currentTerm {
		DPrintf("this is NO.%d server, I refused vote request from %d, arg term = %d, my term = %d\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		reply.VoteGranted = false
	} else if rf.votedFor != - 1 && rf.votedFor != args.CandidateId{
		DPrintf("this is NO.%d server, I refused vote request from %d, I voted for %d\n", rf.me, args.CandidateId, rf.votedFor)
		reply.VoteGranted = false
	} else if args.LastLogTerm < localLastLogTerm || (args.LastLogTerm == localLastLogTerm && args.LastLogIndex < len(rf.log) - 1) {
		DPrintf("this is NO.%d server, I refused vote request from %d, arg log term = %d, my log term = %d, arg log idx = %d, my log idx = %d\n", rf.me, args.CandidateId, args.LastLogTerm, localLastLogTerm, args.LastLogIndex, len(rf.log) - 1)
		reply.VoteGranted = false
	} else {
		rf.votedFor = args.CandidateId
		rf.lastHeartbeatTime = time.Now().UnixNano()
		reply.VoteGranted = true
		rf.persist()
	}
	rf.mu.Unlock()
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
		if rf.voteNum > len(rf.peers)/2 {
			//fmt.Printf("this is NO.%d server, I become the leader, the term is %d, my last idx = %d\n", rf.me, rf.currentTerm, len(rf.log) - 1)
			rf.status = LEADER
			rf.lastHeartbeatTime = time.Now().UnixNano()
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = 0
			}
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
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log) - 1].Term,
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
			rf.persist()
			DPrintf("this is NO.%d server, start election,term = %d\n", rf.me, rf.currentTerm)
			go rf.startElection()
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * CHECK_TIMEOUT_INTERVAL)
	}
}
