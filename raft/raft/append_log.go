package raft

import (
	"fmt"
	"math"
	"time"
)

type Entry struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term                       int
	Success                    bool
	InconsistentTermFirstIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	// 收到请求的 term 比自己当前的 term 大，转变为 follower
	if args.Term > rf.currentTerm {
		rf.updateTermAndStatus(args.Term)
	}
	reply.Term = rf.currentTerm
	rf.lastHeartbeatTime = time.Now().UnixNano()

	// 如果请求的 term 比自己当前的 term 小或者一致性检查失败，拒绝请求
	if args.Term < rf.currentTerm {
		DPrintf("this is NO.%d server, append entries from %d failed, args Term = %d, my Term = %d\n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		reply.Success = false
		rf.mu.Unlock()
		return
	} else if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		i := int(math.Min(float64(args.PrevLogIndex), float64(len(rf.log) - 1)))
		for i - 1 >= 0 && rf.log[i - 1].Term == rf.log[i].Term {
			i--
		}
		reply.InconsistentTermFirstIndex = i
		rf.mu.Unlock()
		return
	}

	rf.status = FOLLOWER
	reply.Success = true
	if args.PrevLogIndex+len(args.Entries) <= rf.lastApplied {
		rf.mu.Unlock()
		return
	}

	// 一致性检查成功，去掉与 leader 不匹配的多余条目

	truncateIndex := args.PrevLogIndex + 1
	newEntryIndex := 0
	for truncateIndex < len(rf.log) && newEntryIndex < len(args.Entries) && rf.log[truncateIndex].Term == args.Entries[newEntryIndex].Term {
		truncateIndex++
		newEntryIndex++
	}
	if newEntryIndex < len(args.Entries) {
		rf.log = rf.log[:truncateIndex]
		DPrintf("this is NO.%d server, Synchronize with leader %d ,my last log is = %d\n", rf.me, args.LeaderId, len(rf.log)-1)
	}


	for i := newEntryIndex; i < len(args.Entries); i++ {
		rf.log = append(rf.log, args.Entries[i])
		DPrintf("this is NO.%d server, append entries from %d success, my log idx = %d\n", rf.me, args.LeaderId, len(rf.log)-1)
	}

	// 如果发现有更新的已被提交的日志，则 apply 本地日志
	rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.log)-1)))
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		fmt.Printf("this is NO.%d server, append entries from %d success, start apply, apply log idx = %d, log term = %d, current term = %d\n", rf.me, args.LeaderId, rf.lastApplied, rf.log[rf.lastApplied].Term, rf.currentTerm)
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- applyMsg
		rf.persist()
	}
	if len(args.Entries) > 0 {
		rf.persist()
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		//go rf.sendAppendEntries(server, args, reply)
		return
	}

	rf.mu.Lock()

	// 收到回复的 term 比自己当前的 term 大，转变为 follower
	if reply.Term > rf.currentTerm {
		fmt.Printf("this is NO.%d server, my term = %d, NO.%d server has lager term: %d\n", rf.me, rf.currentTerm, server, reply.Term)
		rf.updateTermAndStatus(reply.Term)
		rf.mu.Unlock()
		return
	}

	// 可能收到回复的时候自己的状态已经不再是leader
	if rf.status != LEADER {
		rf.mu.Unlock()
		return
	}

	if reply.Success && len(args.Entries) > 0 {
		// 增添条目成功，更改 nextIndex 和 matchIndex
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		// 统计增添条目成功的节点数目
		if rf.commitIndex < rf.matchIndex[server] && rf.log[rf.matchIndex[server]].Term == rf.currentTerm {
			replicateSuccessCount := 0
			for i := 0; i < len(rf.peers); i++ {
				if rf.matchIndex[i] >= rf.matchIndex[server] {
					replicateSuccessCount++
				}
			}
			//DPrintf("replicateSuccessCount is %d\n", replicateSuccessCount)
			// 增添条目成功的节点数目刚好大于一半，apply
			if replicateSuccessCount == len(rf.peers)/2+1 {
				rf.commitIndex = rf.matchIndex[server]
				for rf.lastApplied < rf.commitIndex {
					rf.lastApplied++
					fmt.Printf("NO.%d server(leader), commitIndex = %d, commitTerm = %d, apply log idx = %d, log term = %d, current term = %d\n", rf.me, rf.commitIndex, rf.log[rf.commitIndex].Term, rf.lastApplied, rf.log[rf.lastApplied].Term, rf.currentTerm)
					applyMsg := ApplyMsg{
						CommandValid: true,
						Command:      rf.log[rf.lastApplied].Command,
						CommandIndex: rf.lastApplied,
					}
					rf.applyCh <- applyMsg
				}
				rf.persist()
			}
		}
	} else if !reply.Success {
		if reply.InconsistentTermFirstIndex != 0 {
			rf.nextIndex[server] = reply.InconsistentTermFirstIndex
		} else {
			rf.nextIndex[server] = 1
		}
		newArgs := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
			Entries:      rf.log[rf.nextIndex[server]:],
			LeaderCommit: rf.commitIndex,
		}
		newReply := &AppendEntriesReply{}
		DPrintf("this is NO.%d server, retry to send Entries to %d, current term is %d, the start idx = %v\n", rf.me, server, rf.currentTerm, newArgs.PrevLogIndex+1)
		go rf.sendAppendEntries(server, newArgs, newReply)
	}
	rf.mu.Unlock()
}
