package raft

import (
	"time"
)

const HEARTBEAT_INTERVAL = 100

// 每隔100ms进行一次检测，leader发送心跳
func (rf *Raft) heartbeatTicker() {
	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		if rf.status == LEADER {
			rf.sendHeartBeat()
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * HEARTBEAT_INTERVAL)
	}
}

// 向除了自己之外的所有节点发送心跳
func (rf * Raft) sendHeartBeat() {

	for i:= 0; i < len(rf.peers); i++ {
		if i != rf.me {

			args := &AppendEntriesArgs {
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[i] - 1].Term,
				Entries:      rf.log[rf.nextIndex[i]:],
				LeaderCommit: rf.commitIndex,
			}
			reply := &AppendEntriesReply{}

			go rf.sendAppendEntries(i, args, reply)
		}
	}
}