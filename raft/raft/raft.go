package raft

//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"github.com/Gaaakki/6.824/raft/labgob"
	"sync"
	"time"
)
import "sync/atomic"
import "github.com/Gaaakki/6.824/raft/labrpc"

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	applyCh chan ApplyMsg

	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Persistent state on all servers
	currentTerm int
	votedFor    int
	voteNum     int
	log         []Entry
	status      int

	// Volatile state on all servers
	commitIndex       int
	lastApplied       int
	lastHeartbeatTime int64

	//Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	term, isLeader := rf.currentTerm, rf.status == LEADER
	rf.mu.Unlock()
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.lastApplied)
	e.Encode(rf.log)
	//fmt.Println("write", rf.me, rf.currentTerm, rf.votedFor, rf.log )
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var lastApplied int
	var log []Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&log) != nil {
		fmt.Printf("readPersist error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lastApplied = lastApplied
		rf.log = log
		//fmt.Println("read", rf.me, rf.currentTerm, rf.votedFor, rf.lastApplied, rf.log )
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	index, term, isLeader := len(rf.log), rf.currentTerm, rf.status == LEADER
	if isLeader {

		newEntry := Entry{
			Term:    term,
			Command: command,
		}

		// leader 先自己写日志，改matchIndex是为了统计之后日志写成功的副本数
		rf.log = append(rf.log, newEntry)
		rf.persist()
		rf.matchIndex[rf.me] = rf.nextIndex[rf.me]
		rf.nextIndex[rf.me]++

		// 分发新的条目
		go rf.replicateEntries()
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

func (rf *Raft) replicateEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {

			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
				Entries:      rf.log[rf.nextIndex[i]:],
				LeaderCommit: rf.commitIndex,
			}
			reply := &AppendEntriesReply{}

			//DPrintf("NO.%d server send append entries to NO.%d server, current term is %d, the start idx = %v, Entries length = %d\n", rf.me, i, rf.currentTerm, rf.nextIndex[i], len(args.Entries))
			go rf.sendAppendEntries(i, args, reply)
		}
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) updateTermAndStatus(newTerm int) {
	rf.status = FOLLOWER
	rf.voteNum = 0
	rf.votedFor = -1
	rf.currentTerm = newTerm
	rf.lastHeartbeatTime = time.Now().UnixNano()
	rf.persist()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.status = FOLLOWER
	rf.lastHeartbeatTime = time.Now().UnixNano()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	emptyEntry := Entry{
		Term:    0,
		Command: nil,
	}
	rf.log = append(rf.log, emptyEntry)

	//DPrintf("this is NO.%d server, I'm new server\n", rf.me)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.heartbeatTicker()
	go rf.electionTicker()

	return rf
}
