package raft

import (
	"math/rand"
	"time"
)

const CHECK_TIMEOUT_INTERVAL = 50

// 请求投票rpc的参数
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// 请求投票rpc的返回值
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
	localLastLogTerm := rf.log[len(rf.log)-1].Term
	if args.Term < rf.currentTerm {
		DPrintf("NO.%d server, I refused vote for %d, args term = %d, my term = %d\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		reply.VoteGranted = false
	} else if rf.votedFor != - 1 && rf.votedFor != args.CandidateId {
		DPrintf("NO.%d server, I refused vote for %d, I voted for %d\n", rf.me, args.CandidateId, rf.votedFor)
		reply.VoteGranted = false
	} else if args.LastLogTerm < localLastLogTerm || (args.LastLogTerm == localLastLogTerm && args.LastLogIndex < len(rf.log)-1) {
		DPrintf("NO.%d server, I refused vote for %d, arg log term = %d, my log term = %d, arg log idx = %d, my log idx = %d\n", rf.me, args.CandidateId, args.LastLogTerm, localLastLogTerm, args.LastLogIndex, len(rf.log)-1)
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

	rf.mu.Lock()
	// rpc调用失败或者返回值过期
	if !ok || reply.Term != rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	// 返回值的term大于当前term，主动成为follower
	if reply.Term > rf.currentTerm {
		rf.updateTermAndStatus(reply.Term)
		rf.mu.Unlock()
		return
	}

	// 收到选票
	if reply.VoteGranted && rf.status == CANDIDATE {
		rf.voteNum++
		if rf.voteNum > len(rf.peers)/2 {
			DPrintf("NO.%d server become the leader, status is %v\n", rf.me, rf)
			rf.status = LEADER
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = 0
			}
			rf.sendHeartBeat()
		}
	}
	rf.mu.Unlock()
}

// 开始选举，选举时发送给各节点的参数是相同的
func (rf *Raft) startElection() {
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			reply := &RequestVoteReply{}
			go rf.sendRequestVote(i, args, reply)
		}
	}
}

// 每隔50ms检查一次是否发生了选举超时或心跳超时
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
			rf.startElection()
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * CHECK_TIMEOUT_INTERVAL)
	}
}
