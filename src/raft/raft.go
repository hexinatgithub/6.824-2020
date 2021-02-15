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
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

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

type Term int
type LogIndex int
type status string

const (
	leader    = status("Leader")
	candidate = status("Candidate")
	follower  = status("Follower")
)

const heartBeatInterval = 100 * time.Millisecond

func randLeaderTimeOut() time.Duration {
	return time.Duration(2+rand.Intn(5)) * heartBeatInterval
}

type Log struct {
	Term    Term
	Index   LogIndex
	Command interface{}
}

func makeLog() Log {
	return Log{
		Term:  0,
		Index: 0,
	}
}

type LogEntries struct {
	Entries []Log
}

func (l *LogEntries) Get(i LogIndex) Log {
	if i <= 0 {
		return makeLog()
	}
	return l.Entries[i-1]
}

func (l *LogEntries) TermFirst(i LogIndex) (LogIndex, Term) {
	log := l.Get(i)
	for j := i; j > 0; j-- {
		tmp := l.Get(j)
		if tmp.Term != log.Term {
			break
		}
		log = tmp
	}
	return log.Index, log.Term
}

func (l *LogEntries) Tail(start LogIndex) []Log {
	length := l.Length()
	if int(start) > length {
		return nil
	}
	index := 0
	if start <= 1 {
		index = 0
	} else {
		index = int(start) - 1
	}
	result := make([]Log, length-index)
	copy(result, l.Entries[index:])
	return result
}

func (l *LogEntries) Last() (Log, bool) {
	length := l.Length()
	if length == 0 {
		return makeLog(), true
	}
	return l.Entries[length-1], false
}

func (l *LogEntries) Length() int {
	return len(l.Entries)
}

func (l *LogEntries) TruncateLogs(start, end LogIndex) int {
	var si, ei int
	if start <= 1 {
		si = 0
	} else {
		si = int(start) - 1
	}
	length := l.Length()
	if length >= int(end) {
		ei = int(end) - 1
	} else {
		ei = length
	}

	newLength := truncateLength(si, ei)
	if newLength == 0 {
		return 0
	}
	// avoid memory leak
	if si > 0 {
		tmp := l.Entries[si:ei]
		l.Entries = make([]Log, newLength)
		copy(l.Entries, tmp)
	} else { // logs always grow, no need to delete slice tail
		l.Entries = l.Entries[si:ei]
	}
	return length - newLength
}

func (l *LogEntries) TermLastEntry(firstIndex LogIndex) LogIndex {
	lastLog := l.Get(firstIndex)
	length := l.Length()
	for i := firstIndex + 1; i <= LogIndex(length); i++ {
		tmp := l.Get(i)
		if tmp.Term != lastLog.Term {
			break
		}
		lastLog = tmp
	}
	return lastLog.Index
}

func truncateLength(start, end int) int {
	length := end - start
	if length <= 0 {
		return 0
	}
	return length
}

func (l *LogEntries) Append(logs ...Log) {
	l.Entries = append(l.Entries, logs...)
}

func (l *LogEntries) LastIndexAndTerm() (LogIndex, Term) {
	lastLog, _ := l.Last()
	return lastLog.Index, lastLog.Term
}

type leaderSpecState struct {
	nextIndex  []LogIndex
	matchIndex []LogIndex
}

func (s *leaderSpecState) MaxMajorityIndex() LogIndex {
	counts := make(map[LogIndex]int)
	totalPeers := len(s.matchIndex)
	for i := 0; i < totalPeers; i++ {
		matchIndex := s.matchIndex[i]
		counts[matchIndex] = counts[matchIndex] + 1
	}

	var max LogIndex = 0
	for li, c := range counts {
		if getQuorum(totalPeers, c) && li > max {
			max = li
		}
	}
	return max
}

func newLeaderSpecState(peers int) *leaderSpecState {
	return &leaderSpecState{
		nextIndex:  make([]LogIndex, peers),
		matchIndex: make([]LogIndex, peers),
	}
}

type leaderHeartBeatRecorder struct {
	time *time.Time
}

func (r *leaderHeartBeatRecorder) Elapsed(d time.Duration) bool {
	if r.time == nil {
		return true
	}
	now := time.Now()
	return now.Sub(*r.time) > d
}

func (r *leaderHeartBeatRecorder) Record() {
	now := time.Now()
	r.time = &now
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state
	currentTerm Term
	voteFor     *int
	Logs        LogEntries

	// volatile state
	status      status
	commitIndex LogIndex
	lastApplied LogIndex
	applyChan   chan<- ApplyMsg
	// leader spec volatile state
	*leaderSpecState

	leaderHeartBeatRecorder leaderHeartBeatRecorder
	commitLogCond           *sync.Cond
	isLeaderCond            *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	term := int(rf.currentTerm)
	isleader := rf.status == leader
	rf.mu.Unlock()
	return term, isleader
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
	debugPrefix := logPrefix("persist", rf.me)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(voteToString(rf.voteFor)) != nil ||
		e.Encode(rf.Logs) != nil {
		DPrintf("%s persist raft state error", debugPrefix)
		return
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func voteToString(vote *int) string {
	if vote == nil {
		return "None"
	}
	return fmt.Sprintf("%d", *vote)
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
	debugPrefix := logPrefix("readPersist", rf.me)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm Term
	var voteFor string
	var logs LogEntries
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil {
		DPrintf("%s read persist data error", debugPrefix)
		return
	}
	rf.currentTerm = currentTerm
	rf.voteFor = stringToVote(voteFor)
	rf.Logs = logs
}

func stringToVote(vote string) *int {
	if vote == "None" {
		return nil
	}
	v, err := strconv.Atoi(vote)
	DPrintf("%s convert vote string to int error: %s", "stringToVote", err)
	return &v
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         Term
	Candidate    int
	LastLogIndex LogIndex
	LastLogTerm  Term
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        Term
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	debugPrefix := logPrefix("RequestVote", rf.me)
	if args.Term < rf.currentTerm {
		DPrintf("%s refuse vote for %d, current term: %d, %d candidate term: %d",
			debugPrefix, args.Candidate, rf.currentTerm, args.Candidate, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if rf.voteFor != nil && args.Term == rf.currentTerm && *rf.voteFor == args.Candidate {
		DPrintf("%s already grant vote for %d, current status: %s, current term: %d, candidate term: %d",
			debugPrefix, args.Candidate, rf.status, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}

	var changed bool
	defer func() {
		if changed {
			rf.persist()
		}
	}()
	if args.Term > rf.currentTerm {
		DPrintf("%s %d candidate term is more up date, convert to follower, current term: %d, candidate term: %d, current status: %s",
			debugPrefix, args.Candidate, rf.currentTerm, args.Term, rf.status)
		changed = rf.convertToFollower(args.Term)
	} else if rf.voted() { // && args.Term == rf.currentTerm
		DPrintf("%s already vote for %d term, refuse %d candidate requestVote, current status: %s, vote for: %s",
			debugPrefix, rf.currentTerm, args.Candidate, rf.status, voteToString(rf.voteFor))
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if !rf.voted() && rf.atLeastUpToDate(args.LastLogIndex, args.LastLogTerm) { // && args.Term == rf.currentTerm
		DPrintf("%s candidate log is at least up to date with me, grant vote for %d, current status: %s, current term: %d, candidate term: %d",
			debugPrefix, args.Candidate, rf.status, rf.currentTerm, args.Term)
		rf.vote(args.Candidate)
		changed = true
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}
	DPrintf("%s refuse vote for %d candidate, current vote for: %s, current term: %d",
		debugPrefix, args.Candidate, voteToString(rf.voteFor), rf.currentTerm)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

// atLeastUpToDate return true if candidate log is at least up to date
func (rf *Raft) atLeastUpToDate(lastLogIndex LogIndex, lastLogTerm Term) bool {
	debugPrefix := logPrefix("atLeastUpToDate", rf.me)
	last, empty := rf.Logs.Last()
	if empty {
		DPrintf("%s logs is empty, log matched", debugPrefix)
		return true
	}
	DPrintf("%s last log entry (index: %d, term: %d), leader last entry (index: %d, term: %d)",
		debugPrefix, last.Index, last.Term, lastLogIndex, lastLogTerm)
	if last.Term != lastLogTerm {
		return lastLogTerm > last.Term
	}
	lastIndex, _ := rf.Logs.LastIndexAndTerm()
	return lastLogIndex >= lastIndex
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

func (rf *Raft) leaderElectionPeriodicCheck() {
	debugPrefix := logPrefix("leaderElectionPeriodicCheck", rf.me)
	sendRequestVoteIfLeaderElectionNotTimeout := func(duration time.Duration) {
		if rf.leaderHeartBeatRecorder.Elapsed(duration) {
			rf.vote(rf.me)
			rf.currentTerm++
			rf.persist()
			rf.status = candidate
			rf.leaderSpecState = nil
			count := 1
			lastIndex, lastTerm := rf.Logs.LastIndexAndTerm()
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				Candidate:    rf.me,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}
			DPrintf("%s become candidate, current term: %d", debugPrefix, rf.currentTerm)

			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(i int) {
					reply := &RequestVoteReply{}
					DPrintf("%s sendRequestVote to %d peer", debugPrefix, i)
					if rf.sendRequestVote(i, args, reply) {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if rf.killed() {
							DPrintf("%s stop process request reply from %d peer", debugPrefix, i)
							return
						}

						if rf.status != candidate || reply.Term < rf.currentTerm {
							DPrintf("%s reply from %d peer out of date, raft current status: %s, current term: %d, reply term: %d",
								debugPrefix, i, rf.status, rf.currentTerm, reply.Term)
							return
						}

						if reply.Term > rf.currentTerm {
							DPrintf("%s reply from %d peer term is more update to date, current term: %d, reply term: %d",
								debugPrefix, i, rf.currentTerm, reply.Term)
							if rf.convertToFollower(reply.Term) {
								rf.persist()
							}
							return
						}

						if reply.VoteGranted {
							DPrintf("%s receive vote from %d peer for %d term", debugPrefix, i, reply.Term)
							count++
							if getQuorum(len(rf.peers), count) {
								rf.convertToLeader()
							}
							return
						}
						DPrintf("%s refuse vote from %d peer for %d term", debugPrefix, i, reply.Term)
						return
					}
					DPrintf("%s sendRequestVote to %d peer timeout", debugPrefix, i)
				}(i)
			}
		}
	}
	go func() {
		DPrintf("%s start running election period check", debugPrefix)
		for {
			if rf.killed() {
				DPrintf("%s raft killed, exist", debugPrefix)
				return
			}
			duration := randLeaderTimeOut()
			time.Sleep(duration)
			if rf.killed() {
				DPrintf("%s raft killed after sleep %d duration, exist", debugPrefix, duration)
				return
			}

			func() {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.killed() {
					DPrintf("%s raft killed after lock hold, exist", debugPrefix)
					return
				}

				if rf.status == leader {
					DPrintf("%s raft status is %s, skip sendRequestVote", debugPrefix, rf.status)
					return
				}
				sendRequestVoteIfLeaderElectionNotTimeout(duration)
			}()
		}
	}()
}

type AppendEntriesArgs struct {
	Term         Term
	LeaderID     int
	PrevLogIndex LogIndex
	PrevLogTerm  Term
	Entries      []Log
	LeaderCommit LogIndex
}

type AppendEntriesReply struct {
	Term    Term
	Success bool
	LogRollBack
}

type LogRollBack struct {
	XTerm  Term
	XIndex LogIndex
	XLen   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	debugPrefix := logPrefix("AppendEntries", rf.me)
	if args.Term < rf.currentTerm {
		DPrintf("%s raft is more update to date, raft current term: %d, appendEntriesArgsTerm: %d",
			debugPrefix, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	changed := rf.convertToFollowerAndRecordHeartBeat(args.Term, true)
	defer func() {
		if changed {
			rf.persist()
		}
	}()
	DPrintf("%s receive heartbeat from %d at time [%s]", debugPrefix, args.LeaderID, rf.leaderHeartBeatRecorder.time)
	if rollBack, matched := rf.logMatch(args.PrevLogIndex, args.PrevLogTerm); !matched {
		DPrintf("%s log not matched, refuse append log entries", debugPrefix)
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.LogRollBack = rollBack
		return
	}
	lastIndex, _ := rf.Logs.LastIndexAndTerm()
	// logMatching property means args.PrevLogIndex must equal or less than lastIndex and
	// we can safety drop any log entries after prevLogIndex
	if args.PrevLogIndex != lastIndex {
		deleted := rf.Logs.TruncateLogs(1, args.PrevLogIndex+1)
		DPrintf("%s truncate raft log entries [%d:%d), %d log entries deleted",
			debugPrefix, 1, args.PrevLogIndex+1, deleted)
		changed = changed || deleted > 0
	}
	rf.Logs.Append(args.Entries...)
	changed = changed || len(args.Entries) > 0
	if args.LeaderCommit > rf.commitIndex {
		lastIndex, _ = rf.Logs.LastIndexAndTerm()
		rf.commitIndex = minLogIndex(args.LeaderCommit, lastIndex)
		// weak up commit goroutine after lock release
		defer rf.commitLogCond.Signal()
		DPrintf("%s commitIndex set to %d", debugPrefix, rf.commitIndex)
	}
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) convertToFollower(leaderTerm Term) bool {
	debugPrefix := logPrefix("convertToFollower", rf.me)
	DPrintf("%s begin convert to follower status, current status: %s, candidate or leader term: %d",
		debugPrefix, rf.status, leaderTerm)
	return rf.convertToFollowerAndRecordHeartBeat(leaderTerm, false)
}

func (rf *Raft) convertToFollowerAndRecordHeartBeat(term Term, record bool) (changed bool) {
	debugPrefix := logPrefix("convertToFollowerAndRecordHeartBeat", rf.me)
	if rf.currentTerm != term && rf.voted() {
		DPrintf("%s reset vote for new term: %d, current term is %d, voted for: %s",
			debugPrefix, term, rf.currentTerm, voteToString(rf.voteFor))
		rf.resetVote()
		changed = true
	}
	if record {
		rf.leaderHeartBeatRecorder.Record()
	}
	changed = changed || rf.currentTerm != term
	rf.currentTerm = term
	rf.status = follower
	rf.leaderSpecState = nil
	return
}

// must hold rf.mu and status must be candidate
func (rf *Raft) convertToLeader() {
	debugPrefix := logPrefix("convertToLeader", rf.me)
	rf.status = leader
	rf.leaderSpecState = newLeaderSpecState(len(rf.peers))
	lastIndex, _ := rf.Logs.LastIndexAndTerm()
	nextIndex := lastIndex + 1
	for s := 0; s < len(rf.peers); s++ {
		rf.leaderSpecState.nextIndex[s] = nextIndex
		if rf.me == s {
			rf.leaderSpecState.matchIndex[s] = lastIndex
		} else {
			rf.leaderSpecState.matchIndex[s] = 0
		}
	}
	rf.isLeaderCond.Signal()
	DPrintf("%s converted to leader, current term: %d, nextIndex initial to %d, matchIndex all initial to %d",
		debugPrefix, rf.currentTerm, nextIndex, 0)
}

func (rf *Raft) logMatch(prevLogIndex LogIndex, prevLogTerm Term) (rollBack LogRollBack, matched bool) {
	debugPrefix := logPrefix("logMatch", rf.me)
	defer func() {
		if err := recover(); err != nil {
			DPrintf("%s prevLogIndex: %d, prevLogTerm: %d, current raft logs length: %d, error: %s",
				debugPrefix, prevLogIndex, prevLogTerm, rf.Logs.Length(), err)
		}
	}()
	lastIndex, _ := rf.Logs.LastIndexAndTerm()
	if lastIndex < prevLogIndex {
		DPrintf("%s log not match, raft logs length is less than leader, current logs length %d, prevLogIndex: %d, prevLogTerm: %d",
			debugPrefix, rf.Logs.Length(), prevLogIndex, prevLogTerm)
		rollBack.XLen = rf.Logs.Length()
		return
	}
	log := rf.Logs.Get(prevLogIndex)
	matched = log.Index == prevLogIndex && log.Term == prevLogTerm
	DPrintf("%s log matched %t, raft log(index: %d, term: %d), prevLogIndex: %d, prevLogTerm: %d",
		debugPrefix, matched, log.Index, log.Term, prevLogIndex, prevLogTerm)
	if !matched {
		rollBack.XLen = rf.Logs.Length()
		rollBack.XIndex, rollBack.XTerm = rf.Logs.TermFirst(prevLogIndex)
	}
	return
}

func (rf *Raft) vote(i int) {
	rf.voteFor = &i
}

func (rf *Raft) voted() bool {
	return rf.voteFor != nil
}

func (rf *Raft) resetVote() {
	rf.voteFor = nil
}

func (rf *Raft) commitLogEntries() {
	debugPrefix := logPrefix("commitLogEntries", rf.me)
	go func() {
		rf.commitLogCond.L.Lock()
		// when server first start, just wait to commit logs
		rf.commitLogCond.Wait()
		defer rf.commitLogCond.L.Unlock()
		for {
			if rf.killed() {
				DPrintf("%s raft is killed, exist", debugPrefix)
				return
			}
			if rf.commitIndex > rf.lastApplied {
				DPrintf("%s committing log from %d up to %d", debugPrefix, rf.lastApplied+1, rf.commitIndex)
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					log := rf.Logs.Get(i)
					rf.applyChan <- ApplyMsg{
						CommandValid: true,
						Command:      log.Command,
						CommandIndex: int(log.Index),
					}
					DPrintf("%s send applyMsy, log index: %d, log term: %d, command: %#v",
						debugPrefix, log.Index, log.Term, log.Command)
				}
				rf.lastApplied = rf.commitIndex
			}
			rf.commitLogCond.Wait()
		}
	}()
}

func (rf *Raft) sendHeartBeatPeriodIfIsLeader() {
	debugPrefix := logPrefix("sendHeartBeatPeriodIfIsLeader", rf.me)
	go func() {
		if rf.killed() {
			DPrintf("%s raft is killed, stop sendAppendEntries and exist", debugPrefix)
			return
		}

		for {
			rf.isLeaderCond.L.Lock()
		Top:
			if rf.killed() {
				DPrintf("%s raft is killed after hold mutex lock, exist", debugPrefix)
				rf.isLeaderCond.L.Unlock()
				return
			}
			if rf.status != leader {
				DPrintf("%s raft status is %s, stop sendAppendEntries to all peers", debugPrefix, rf.status)
				goto Wait
			}
			DPrintf("%s send heartbeat to all peers, current term: %d", debugPrefix, rf.currentTerm)
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				nextIndex := rf.leaderSpecState.nextIndex[i]
				prevLog := rf.Logs.Get(nextIndex - 1)
				appendEntries := rf.Logs.Tail(nextIndex)
				heartBeat := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: prevLog.Index,
					PrevLogTerm:  prevLog.Term,
					Entries:      appendEntries,
					LeaderCommit: rf.commitIndex,
				}
				go func(i int) {
					DPrintf("%s sendAppendEntries to %d peer", debugPrefix, i)
					reply := new(AppendEntriesReply)
					if rf.sendAppendEntries(i, heartBeat, reply) {
						if rf.killed() {
							DPrintf("%s raft is killed, not process reply from %d peer", debugPrefix, i)
							return
						}
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if rf.killed() {
							DPrintf("%s raft is killed, not process reply from %d peer after hold lock", debugPrefix, i)
							return
						}
						if rf.status != leader {
							DPrintf("%s raft status is %s now, not process reply from %d peer", debugPrefix, rf.status, i)
							return
						}

						if reply.Term > rf.currentTerm {
							DPrintf("%s %d peer reply is more update to date, current term: %d, reply term: %d",
								debugPrefix, i, rf.currentTerm, reply.Term)
							if rf.convertToFollower(reply.Term) {
								rf.persist()
							}
							return
						}
						if reply.Success {
							newNextIndex := nextIndex + LogIndex(len(appendEntries))
							rf.leaderSpecState.matchIndex[i] = newNextIndex - 1
							rf.leaderSpecState.nextIndex[i] = newNextIndex
							commitIndex := rf.leaderSpecState.MaxMajorityIndex()
							DPrintf("%s set %d peer nextIndex to %d and matchIndex to %d",
								debugPrefix, i, rf.leaderSpecState.nextIndex[i], rf.leaderSpecState.matchIndex[i])
							DPrintf("%s matchIndex: %v, nextIndex: %v, commitIndex: %d",
								debugPrefix, rf.leaderSpecState.matchIndex, rf.leaderSpecState.nextIndex, commitIndex)
							// commit log up to commitIndex if commitIndex > rf.commitIndex and
							// commitIndex logEntry's term is equal to rf.currentTerm
							if commitIndex > rf.lastApplied {
								commitLog := rf.Logs.Get(commitIndex)
								if commitLog.Term == rf.currentTerm {
									DPrintf("%s trying to commit log up to %d", debugPrefix, commitIndex)
									rf.commitIndex = commitIndex
									defer rf.commitLogCond.Signal()
									return
								}
								DPrintf("%s commit %d log's term %d is not same with current term %d, commit abort",
									debugPrefix, commitIndex, commitLog.Term, rf.currentTerm)
							}
						} else {
							rf.leaderSpecState.nextIndex[i]--
							DPrintf("%s %d peer log not match, set nextIndex to %d", debugPrefix, i, rf.leaderSpecState.nextIndex[i])
						}
						return
					}
					rf.mu.Lock()
					DPrintf("%s sendAppendEntries to %d peer timeout, current term: %d, request term: %d",
						debugPrefix, i, rf.currentTerm, heartBeat.Term)
					rf.mu.Unlock()
				}(i)
			}
			rf.mu.Unlock()
			time.Sleep(heartBeatInterval)
			continue
		Wait:
			DPrintf("%s wait to become leader", debugPrefix)
			rf.isLeaderCond.Wait()
			DPrintf("%s weak up, should be a leader, current status: %s", debugPrefix, rf.status)
			goto Top
		}
	}()
}

func (rf *Raft) rollBackTo(rollBack LogRollBack) (nextIndex LogIndex) {
	length := rf.Logs.Length()
	if rollBack.XLen < length {
		nextIndex = LogIndex(rollBack.XLen + 1)
		return
	}
	//log := rf.Logs.Get(rollBack.XIndex)
	//if log.Term != rollBack.XTerm {
	//	nextIndex = rollBack.XIndex
	//	return
	//}
	//nextIndex = rf.Logs.TermLastEntry(rollBack.XIndex) + 1
	nextIndex = -1
	return
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
	var index LogIndex = 0
	var term Term = 0

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	debugPrefix := logPrefix("Start", rf.me)
	if rf.status != leader {
		DPrintf("%s not a leader, refuse receive command", debugPrefix)
		return int(index), int(term), false
	}
	lastIndex, _ := rf.Logs.LastIndexAndTerm()
	index = lastIndex + 1
	term = rf.currentTerm
	log := Log{
		Term:    term,
		Index:   index,
		Command: command,
	}
	rf.Logs.Append(log)
	rf.persist()
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	DPrintf("%s append log at %d index, term: %d", debugPrefix, log.Index, log.Term)
	return int(index), int(term), true
}

//
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.commitLogCond.Signal()
	rf.isLeaderCond.Signal()
	rf.persist()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

	// Your initialization code here (2A, 2B, 2C).
	rf.applyChan = applyCh
	rf.currentTerm = 0
	rf.status = follower
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.commitLogCond = sync.NewCond(&rf.mu)
	rf.isLeaderCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// kick off leader election periodically
	rf.leaderElectionPeriodicCheck()
	rf.commitLogEntries()
	rf.sendHeartBeatPeriodIfIsLeader()
	return rf
}

// ---------------------------------------------
// helper function

func minLogIndex(a, b LogIndex) LogIndex {
	if a < b {
		return a
	}
	return b
}

func logPrefix(method string, rid int) string {
	return fmt.Sprintf("[raft %d] %s#", rid, method)
}

func majority(peers int) int {
	return peers/2 + 1
}

func getQuorum(peers, count int) bool {
	return count >= majority(peers)
}
