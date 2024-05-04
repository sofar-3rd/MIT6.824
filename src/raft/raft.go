package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
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
	"labs-2020/src/labgob"
	"sync"
	"time"
)
import "sync/atomic"
import "labs-2020/src/labrpc"

// ApplyMsg
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
	CommandTerm  int
	CommandIndex int
}

// Raft
// A Go object implementing a single Raft peer.
//

type nodeState uint32

const (
	leader    nodeState = 1
	candidate nodeState = 2
	follower  nodeState = 3
)

type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent state for all servers
	currentTerm int
	votedFor    int
	logEntries  []Entry
	// Volatile state on all servers
	commitIndex int
	lastApplied int
	// Volatile state on leaders
	// 当节点为 leader 时, 维护其他节点日志同步情况
	nextIndex  []int //选举成功时初始化为最后一个日志的 Index
	matchIndex []int
	// 定时器
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	// 状态
	state nodeState

	applyCh        chan ApplyMsg
	applyCond      *sync.Cond   // used to wakeup applier goroutine after committing new entries
	replicatorCond []*sync.Cond // used to signal replicator goroutine to batch replicating entries
}

type Entry struct {
	Term  int
	Index int
	Cmd   interface{}
}

func (rf *Raft) changeState(state nodeState) {
	// 当选 leader 后，所有的 nextIndex 设置为最后一条日志的 Index
	rf.state = state
	if state == leader {
		// rf.appendNewEntry("")
		for i := range rf.nextIndex {
			rf.nextIndex[i] = rf.getLastLog().Index + 1
		}
		for i := range rf.matchIndex {
			rf.matchIndex[i] = 0
		}
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state != leader {
				DPrintf("{Node %v} election timeout, start election", rf.me)
				rf.changeState(candidate)
				rf.currentTerm += 1
				rf.startElection()
				rf.electionTimer.Reset(RandomizedElectionTimeout())
			}
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			rf.heartbeatTimer.Reset(HeartbeatTimeout())
			if rf.state == leader {
				rf.BroadcastHeartbeat(true)
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for rf.killed() == false {
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		rf.replicateOneRound(peer)
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == leader && rf.matchIndex[peer] < rf.getLastLog().Index
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		firstIndex, commitIndex, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		entries := make([]Entry, commitIndex-lastApplied)
		copy(entries, rf.logEntries[lastApplied+1-firstIndex:commitIndex+1-firstIndex])
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Cmd,
				CommandTerm:  entry.Term,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		DPrintf("{Node %v} applies entries %v-%v in term %v", rf.me, rf.lastApplied, commitIndex, rf.currentTerm)
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

func (rf *Raft) genVoteRequest() RequestVoteArgs {
	lastLogIndex := rf.getLastLog().Index
	lastLogTerm := rf.getLastLog().Term
	req := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	return req
}

func (rf *Raft) genAppendRequest(prevLogIndex int) AppendEntriesArgs {
	pos := prevLogIndex - rf.getFirstLog().Index
	req := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: rf.logEntries[pos].Index,
		PrevLogTerm:  rf.logEntries[pos].Term,
		Entries:      rf.logEntries[pos+1:],
		LeaderCommit: rf.commitIndex,
	}
	return req
}

func (rf *Raft) startElection() {
	req := rf.genVoteRequest()
	// DPrintf("{Node %v} starts election with RequestVoteRequest %v", rf.me, req)
	grantedVotes := 1
	rf.votedFor = rf.me
	rf.persist()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			res := new(RequestVoteReply)
			if rf.sendRequestVote(peer, &req, res) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// DPrintf("{Node %v} receives RequestVoteResponse %v from {Node %v} after sending RequestVoteRequest %v in term %v", rf.me, res, peer, req, rf.currentTerm)
				if rf.currentTerm == req.Term && rf.state == candidate {
					if res.VoteGranted {
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							DPrintf("{Node %v} receives majority votes in term %v", rf.me, rf.currentTerm)
							rf.changeState(leader)
							rf.BroadcastHeartbeat(true)
						}
					} else if res.Term > rf.currentTerm {
						DPrintf("{Node %v} finds a new leader {Node %v} with term %v and steps down in term %v", rf.me, peer, res.Term, rf.currentTerm)
						rf.changeState(follower)
						rf.currentTerm, rf.votedFor = res.Term, -1
						rf.persist()
					}
				}
			}
		}(peer)

	}
}

func (rf *Raft) BroadcastHeartbeat(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			// need sending at once to maintain leadership
			go rf.replicateOneRound(peer)
		} else {
			// just signal replicator goroutine to send entries in batch
			rf.replicatorCond[peer].Signal()
		}
	}
}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.RLock()
	if rf.state != leader {
		rf.mu.RUnlock()
		return
	}
	prevLogIndex := rf.nextIndex[peer] - 1
	if prevLogIndex < rf.getFirstLog().Index {
		// 使用快照同步
		rf.mu.RUnlock()
	} else {
		// 追加日志同步
		request := rf.genAppendRequest(prevLogIndex)
		rf.mu.RUnlock()
		response := new(AppendEntriesReply)
		if rf.sendAppendEntries(peer, &request, response) {
			rf.mu.Lock()
			rf.handleAppendEntriesResponse(peer, &request, response)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) handleAppendEntriesResponse(peer int, request *AppendEntriesArgs, response *AppendEntriesReply) {

	if response.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = response.Term, -1
		rf.changeState(follower)
		return
	}

	if len(request.Entries) == 0 { //心跳包
		return
	}

	if response.Success {
		rf.matchIndex[peer] = request.Entries[len(request.Entries)-1].Index
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		if rf.matchIndex[peer] > rf.commitIndex {
			rf.updateCommitIndex(rf.matchIndex[peer])
		}
		return
	}

	if response.XTerm == -1 {
		rf.nextIndex[peer] = response.XIndex
		return
	} else {
		conflictTerm := response.XTerm
		conflictTermFirstIndex := response.XIndex
		localFirstIndex := rf.getFirstLog().Index
		if rf.logEntries[conflictTermFirstIndex-localFirstIndex].Term != conflictTerm {
			rf.nextIndex[peer] = response.XIndex
		} else {
			rf.nextIndex[peer] = response.XIndex + 1
			rf.matchIndex[peer] = response.XIndex
			if rf.matchIndex[peer] > rf.commitIndex {
				rf.updateCommitIndex(rf.matchIndex[peer])
			}
		}
	}
}

func (rf *Raft) updateCommitIndex(maxCommitIndex int) {
	commitIndex := rf.commitIndex + 1
	for commitIndex <= maxCommitIndex {
		cnt := 1
		for i := range rf.matchIndex {
			if rf.matchIndex[i] >= commitIndex {
				cnt++
			}
		}
		if cnt <= len(rf.peers)/2 {
			break
		}
		commitIndex++
	}
	if commitIndex-1 > rf.commitIndex {
		rf.commitIndex = commitIndex - 1
		rf.applyCond.Signal()
	}
}

func (rf *Raft) getFirstLog() Entry {
	return rf.logEntries[0]
}

func (rf *Raft) getLastLog() Entry {
	return rf.logEntries[len(rf.logEntries)-1]
}

func (rf *Raft) isLogUpToDate(lastLogTerm int, lastLogIndex int) bool {
	localLastIndex := rf.getLastLog().Index
	localLastTerm := rf.getLastLog().Term

	if localLastTerm > lastLogTerm {
		return false
	} else if localLastTerm < lastLogTerm {
		return true
	}
	return localLastIndex <= lastLogIndex
}

func (rf *Raft) appendNewEntry(command interface{}) Entry {
	newLog := Entry{
		Cmd:   command,
		Term:  rf.currentTerm,
		Index: rf.getLastLog().Index + 1,
	}
	rf.logEntries = append(rf.logEntries, newLog)
	rf.persist()
	return newLog
}

// return currentTerm and whether this server
// believes it is the leader.

func (rf *Raft) GetState() (int, bool) {
	isLeader := false
	term := rf.currentTerm
	if rf.state == leader {
		isLeader = true
	}
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	var w bytes.Buffer
	e := labgob.NewEncoder(&w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)

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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var logEntries []Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logEntries) != nil {
		println("read Persist.raftstate error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logEntries = logEntries
	}
	DPrintf("{Node %v} restore state with currentTerm %v votedFor %v firstLog %v lastLog %v length %v", rf.me, rf.currentTerm, rf.votedFor, rf.getFirstLog(), rf.getLastLog(), len(logEntries))
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
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
	defer rf.persist()
	// defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v} before processing requestVoteRequest %v and reply requestVoteResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, args, reply)

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateID) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.state = follower
		rf.currentTerm, rf.votedFor = args.Term, -1
	}

	// If votedFor is null or candidateId,
	// and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if !rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	rf.votedFor = args.CandidateID
	rf.electionTimer.Reset(RandomizedElectionTimeout())
	reply.Term, reply.VoteGranted = rf.currentTerm, true
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// Follower中与Leader冲突的Log对应的任期号。
	XTerm int
	// Follower中，对应任期号为 XTerm 的第一条 Log 条目的槽位号。
	XIndex int
	// 如果 Follower 在对应位置没有 Log，那么XTerm会返回-1，XLen 表示空白的 Log 槽位数。
	XLen int
}

func (rf *Raft) matchLog(PrevLogTerm int, PrevLogIndex int) bool {
	pos := PrevLogIndex - rf.getFirstLog().Index
	if pos < len(rf.logEntries) && rf.logEntries[pos].Term == PrevLogTerm {
		return true
	}
	return false
}

func (rf *Raft) advanceCommitIndexForFollower(leaderCommitIndex int) {
	commitIndex := Min(leaderCommitIndex, rf.getLastLog().Index)
	if commitIndex > rf.commitIndex {
		rf.commitIndex = commitIndex
		rf.applyCond.Signal()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing AppendEntriesRequest %v and reply AppendEntriesResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = reply.Term, -1
	}

	rf.changeState(follower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	// 流程
	// 获取本地日志，localLastIndex = rf.getLastLog().index  localLastTerm = rf.getLastLog().Term
	// 若 localLastIndex < PreLogIndex，则将 Xterm 置为 -1，表示没有对应位置的日志，并将 XLen 设为对应 PreLogIndex - localLastIndex；
	// 若 localLastIndex >= PreLogIndex，localLastTerm ！= PrevLogTerm，将 XTerm 置为冲突日志的 Term，并将 XIndex 设置为 XTerm 的第一条日志；
	// 若 localLastIndex = PreLogIndex，localLastTerm ！= PrevLogTerm，则添加日志

	firstIndex := rf.getFirstLog().Index
	if args.PrevLogIndex < firstIndex {
		reply.Term, reply.Success = 0, false
		DPrintf("{Node %v} receives unexpected AppendEntriesRequest %v from {Node %v} because prevLogIndex %v < firstLogIndex %v", rf.me, args, args.LeaderID, args.PrevLogIndex, rf.getFirstLog().Index)
	}

	// 日志匹配失败
	if !rf.matchLog(args.PrevLogTerm, args.PrevLogIndex) {
		reply.Term, reply.Success = rf.currentTerm, false
		LastIndex := rf.getLastLog().Index
		if LastIndex < args.PrevLogIndex {
			reply.XTerm, reply.XIndex, reply.XLen = -1, LastIndex+1, args.PrevLogIndex-LastIndex
		} else { // LastIndex >= args.PrevLogIndex
			pos := args.PrevLogIndex - firstIndex
			reply.XTerm = rf.logEntries[pos].Term
			pos_ := pos - 1
			for pos_ >= 0 && rf.logEntries[pos_].Term == rf.logEntries[pos].Term {
				pos_--
			}
			reply.XIndex = firstIndex + pos_ + 1
		}
		return
	}

	// 日志匹配成功

	// 如果 entries 为空，则为心跳包，直接返回即可
	if len(args.Entries) != 0 { // 更新日志
		appendPos := args.Entries[0].Index - firstIndex
		if appendPos < len(rf.logEntries) {
			rf.logEntries = rf.logEntries[0:appendPos]
			rf.logEntries = append(rf.logEntries, args.Entries...)
		} else {
			rf.logEntries = append(rf.logEntries, args.Entries...)
		}
	}
	// 更新 CommitIndex
	rf.advanceCommitIndexForFollower(args.LeaderCommit)
	reply.Term, reply.Success = rf.currentTerm, true
}

type InstallSnapshotRequest struct {
	Term              int
	LeaderID          int
	LastIncludedTerm  int
	LastIncludedIndex int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotResponse struct {
	Term int
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIndex := rf.getFirstLog().Index
	if index <= snapshotIndex {
		DPrintf("{Node %v} rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v", rf.me, index, snapshotIndex, rf.currentTerm)
		return
	}

	rf.logEntries = rf.logEntries[index-snapshotIndex:]
	rf.logEntries[0].Cmd = nil
	// rf.persister.SaveStateAndSnapshot()
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after replacing log with snapshotIndex %v as old snapshotIndex %v is smaller", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), index, snapshotIndex)
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotRequest, reply InstallSnapshotResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} before processing InstallSnapshotRequest %v and reply InstallSnapshotResponse %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), args, reply)

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}

	rf.changeState(follower)
	rf.electionTimer.Reset(RandomizedElectionTimeout())

	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      args.Data,
			CommandTerm:  args.LastIncludedTerm,
			CommandIndex: args.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Lock()
	DPrintf("{Node %v} service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex %v to check whether snapshot is still valid in term %v", rf.me, lastIncludedTerm, lastIncludedIndex, rf.currentTerm)

	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("{Node %v} rejects the snapshot which lastIncludedIndex is %v because commitIndex %v is larger", rf.me, lastIncludedIndex, rf.commitIndex)
		return false
	}

	if lastIncludedIndex > rf.getLastLog().Index {
		rf.logEntries = make([]Entry, 1)
	} else {
		rf.logEntries = rf.logEntries[lastIncludedIndex-rf.getFirstLog().Index:]
		rf.logEntries[0].Cmd = nil
	}
	rf.logEntries[0].Term, rf.logEntries[0].Index = lastIncludedTerm, lastIncludedIndex
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
	// rf.persister.SaveStateAndSnapshot()
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after accepting the snapshot which lastIncludedTerm is %v, lastIncludedIndex is %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), lastIncludedTerm, lastIncludedIndex)
	return true
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	defer rf.mu.Unlock()
	if rf.state != leader {
		return -1, -1, false
	}
	newLog := rf.appendNewEntry(command)
	DPrintf("{Node %v} receives a new command[%v] to replicate in term %v", rf.me, newLog, rf.currentTerm)
	rf.BroadcastHeartbeat(false)
	return newLog.Index, newLog.Term, true
}

//
// Kill
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	// rf.persist()
	DPrintf("{Node %v} is killed with currentTerm %v votedFor %v firstLog %v lastLog %v length %v", rf.me, rf.currentTerm, rf.votedFor, rf.getFirstLog(), rf.getLastLog(), len(rf.logEntries))
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// Make
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
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		state:          follower,
		currentTerm:    0,
		votedFor:       -1,
		logEntries:     make([]Entry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		heartbeatTimer: time.NewTimer(HeartbeatTimeout()),
		electionTimer:  time.NewTimer(RandomizedElectionTimeout()),
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.applyCond = sync.NewCond(&rf.mu)
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			go rf.replicator(i)
		}
	}

	go rf.applier()

	go rf.ticker()
	return rf
}
