use rand::Rng;

use std::sync::Arc;

use futures::channel::mpsc::UnboundedSender;
use futures::executor::ThreadPool;

use futures::lock::{Mutex, MutexGuard};
use futures::{Future, FutureExt};

use futures_timer::Delay;
use std::time::Duration;

use std::cmp::{max, min};

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

mod wait_group;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;
use wait_group::WaitGroup;

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    state: Arc<State>,
    vote: Option<usize>,
    apply_ch: UnboundedSender<ApplyMsg>,
    election_timer: Option<CancellableTask>,
    heartbeat_task: Option<CancellableTask>,
    log: Vec<Entry>,
    commit_index: usize,
    last_applied: usize,
    next_index: Vec<usize>,
    match_index: Vec<usize>,

    append_entries_epoch: usize,
}

#[derive(::prost::Message)]
struct PersistentState {
    #[prost(uint64, tag = "1")]
    current_term: u64,
    #[prost(uint64, optional, tag = "2")]
    voted_for: Option<u64>,
    #[prost(message, repeated, tag = "3")]
    log: Vec<Entry>,
}

struct CancellableTask {
    _sender: futures::channel::oneshot::Sender<()>,
}

struct Select<A, B>(A, B);

use std::pin::Pin;
use std::task::{Context, Poll};

impl<A: Future, B: Future> Future for Select<A, B> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<()> {
        use std::task::Poll::*;
        let unsafe_self = unsafe { Pin::into_inner_unchecked(self) };
        match unsafe { Pin::new_unchecked(&mut unsafe_self.0) }.poll(cx) {
            Ready(_) => Ready(()),
            Pending => match unsafe { Pin::new_unchecked(&mut unsafe_self.1) }.poll(cx) {
                Ready(_) => Ready(()),
                Pending => Pending,
            },
        }
    }
}

impl CancellableTask {
    fn spawn<F: Future + Send + 'static>(executor: &ThreadPool, task: F) -> Self {
        let (sender, receiver) = futures::channel::oneshot::channel();
        executor.spawn_ok(Select(receiver, task));
        CancellableTask { _sender: sender }
    }
}

const ELECTION_TIMEOUT_MIN: u64 = 300;
const ELECTION_TIMEOUT_MAX: u64 = 500;

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        let next_index = peers.iter().map(|_| 0).collect();
        let match_index = peers.iter().map(|_| 0).collect();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: Arc::default(),
            vote: None,
            apply_ch,
            election_timer: None,
            heartbeat_task: None,
            log: vec![],
            commit_index: 0,
            last_applied: 0,
            next_index,
            match_index,
            append_entries_epoch: 0,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        let persistent_state = PersistentState {
            current_term: self.state.term,
            voted_for: self.vote.map(|x| x as u64),
            log: self.log.clone(),
        };
        let mut data = vec![];
        labcodec::encode(&persistent_state, &mut data)
            .expect("message should encode without trouble");
        self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        match labcodec::decode::<PersistentState>(data) {
            Ok(persistent_state) => {
                debug!(
                    "node {} restarts with state: {:?}",
                    self.me, persistent_state
                );
                self.modify_state(|state| {
                    state.is_leader = false;
                    state.term = persistent_state.current_term;
                });
                self.vote = persistent_state.voted_for.map(|x| x as usize);
                self.log = persistent_state.log;
            }
            Err(e) => {
                panic!("{:?}", e);
            }
        }
    }

    fn modify_state(&mut self, f: impl Fn(&mut State) -> ()) {
        let mut state = (*self.state).clone();
        f(&mut state);
        self.state = Arc::new(state);
    }

    fn become_follower(&mut self, term: u64) {
        self.modify_state(|state| {
            state.is_leader = false;
            state.term = term;
        });
        self.persist();
        self.heartbeat_task = None;
    }

    fn has_matching_log_entry(&self, prev_log_index: usize, prev_log_term: u64) -> bool {
        if prev_log_index == 0 {
            return true;
        }
        if prev_log_index > self.log.len() {
            return false;
        }
        return self.log[prev_log_index - 1].term == prev_log_term;
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        self.persist();
        let _ = &self.persister;
        let _ = &self.apply_ch;
        let _ = &self.last_applied;
        let _ = &self.match_index;
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    me: usize,
    name: String,
    raft: Arc<Mutex<Raft>>,
    pool: Arc<ThreadPool>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        let node = Node {
            me: raft.me,
            name: format!("node {}", raft.me),
            raft: Arc::new(Mutex::new(raft)),
            pool: Arc::new(ThreadPool::new().unwrap()),
        };
        let node_ = node.clone();
        node.pool.spawn_ok(async move {
            node_.start_election_timer(&mut node_.clone().raft.lock().await);
        });
        node
    }

    fn start_election_timer(&self, raft: &mut MutexGuard<Raft>) {
        let self_ = self.clone();
        let me = self_.me;
        let initial_term = raft.state.term;
        raft.election_timer = Some(CancellableTask::spawn(&self.pool, async move {
            let mut timeout =
                rand::thread_rng().gen_range(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX);
            debug!(
                "node {}: election timer start for term {} ({})",
                me, initial_term, timeout
            );
            Delay::new(Duration::from_millis(timeout)).await;
            loop {
                debug!("node {}: election timeout ({})", me, timeout);
                let mut raft = self_.raft.lock().await;
                raft.modify_state(|state| {
                    state.is_leader = false;
                    state.term += 1;
                });
                raft.vote = Some(me);
                raft.persist();
                let term = raft.state.term;
                let peers = raft.peers.clone();
                let votes_needed = peers.len() / 2;
                debug!(
                    "node {}: starting election (term {}), waiting for {} votes",
                    me, term, votes_needed
                );
                let last_log_index = raft.log.len() as u64;
                let last_log_term = if last_log_index > 0 {
                    raft.log[last_log_index as usize - 1].term
                } else {
                    0
                } as u64;
                drop(raft);

                let wg = Arc::new(WaitGroup::new(votes_needed));
                for (index, peer) in peers.iter().enumerate() {
                    if index != me {
                        let self_clone = self_.clone();
                        let peer_clone = peer.clone();
                        let wg = wg.clone();
                        peer.spawn(async move {
                            loop {
                                let current_term = self_clone.raft.lock().await.state.term;
                                if current_term != term {
                                    debug!(
                                        "node {}: election task cancelled (term {} -> {})",
                                        me, term, current_term
                                    );
                                    break;
                                }
                                select! {
                                    reply = peer_clone.request_vote(&RequestVoteArgs {
                                        candidate_id: me as u64,
                                        term,
                                        last_log_index,
                                        last_log_term,
                                    }).fuse() => {
                                        match reply {
                                            Ok(RequestVoteReply {
                                                vote_granted: true,
                                                term: reply_term
                                            }) if reply_term == term => {
                                                wg.done();
                                                break;
                                            }
                                            Err(labrpc::Error::Timeout) => {
                                                debug!(
                                                    "node {}: timeout sending RequestVote (term {}) to {}",
                                                    me, term, index
                                                );
                                                continue;
                                            }
                                            _ => {
                                                break;
                                            }
                                        }
                                    }
                                    _ = Delay::new(Duration::from_millis(100)).fuse() => {
                                        debug!(
                                            "node {}: timeout sending RequestVote (term {}) to {}",
                                            me, term, index
                                        );
                                        continue;
                                    }
                                }
                            }
                        });
                    }
                }
                timeout = rand::thread_rng().gen_range(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX);
                debug!(
                    "node {}: election timer start for term {} ({})",
                    me, term, timeout
                );
                select! {
                    _ = Delay::new(Duration::from_millis(timeout)).fuse() => {
                        // continue loop
                    },
                    _ = wg.wait().fuse() => {
                        debug!("node {}: got votes, becoming a leader", me);
                        self_.run_leader(term).await;
                        break;
                    }
                }
            }
        }));
    }

    async fn run_leader(&self, term: u64) {
        let mut raft = self.raft.lock().await;
        if raft.state.term != term {
            warn!(
                "node {}: run_leader: term {} has passed, current is {}",
                self.me, term, raft.state.term
            );
            return;
        }
        raft.modify_state(|state| {
            state.is_leader = true;
        });
        let log_len = raft.log.len();
        for next_index in raft.next_index.iter_mut() {
            *next_index = log_len;
        }
        for match_index in raft.match_index.iter_mut() {
            *match_index = 0;
        }

        let self_ = self.clone();
        raft.heartbeat_task = Some(CancellableTask::spawn(&self.pool, async move {
            loop {
                self_.send_log_entries().await;
                Delay::new(Duration::from_millis(100)).await;
            }
        }));
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message + std::fmt::Debug,
    {
        let self_ = self.clone();
        let mut raft = async_std::task::block_on(self_.raft.lock());
        if !raft.state.is_leader {
            return Err(Error::NotLeader);
        }
        let term = raft.state.term;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).expect("message should encode without trouble");
        let index = raft.log.len() as u64 + 1;
        raft.log.push(Entry { term, command: buf });
        debug!(
            "node {} added log entry {:?} at index {}",
            raft.me, command, index
        );

        let self_1 = self_.clone();
        self_.pool.spawn_ok(async move {
            self_1.send_log_entries().await;
        });

        Ok((index, term))
    }

    async fn send_log_entries(&self) {
        let me = self.me;
        let mut raft = self.raft.lock().await;
        let term = raft.state.term;
        if !raft.state.is_leader {
            warn!(
                "node {}: send_log_entries: no longer a leader, current term is {}",
                me, raft.state.term
            );
            return;
        }
        debug!("node {}: sending log entries (term {})", me, term);

        raft.append_entries_epoch += 1;

        for (peer_index, peer) in raft.peers.iter().enumerate() {
            if peer_index == me {
                continue;
            }
            self.send_log_entries_to(&raft, peer_index, peer.clone());
        }
    }

    fn send_log_entries_to(&self, raft: &Raft, peer_index: usize, peer: RaftClient) {
        let me = self.me;
        let term = raft.state.term;
        let self_ = self.clone();
        let peer_clone = peer.clone();
        let send_index = min(
            raft.log.len() + 1,
            max(
                raft.next_index[peer_index],
                raft.match_index[peer_index] + 1,
            ),
        );
        let msg = AppendEntriesArgs {
            term,
            prev_log_index: send_index.saturating_sub(1) as u64,
            prev_log_term: if send_index <= 1 {
                0
            } else {
                raft.log[send_index - 2].term as u64
            },
            entries: if send_index == 0 {
                vec![]
            } else {
                raft.log[send_index - 1..].to_vec()
            },
            leader_commit: raft.commit_index as u64,
        };
        let match_index = min(raft.log.len(), send_index + msg.entries.len());
        debug!(
            "node {}: send_index[{}] = {}, match_index = {}",
            me, peer_index, send_index, match_index
        );
        let start_epoch = raft.append_entries_epoch;
        peer.spawn(async move {
            if let Ok(AppendEntriesReply {
                term: reply_term,
                success,
            }) = peer_clone.append_entries(&msg).await
            {
                if reply_term < term {
                    warn!("node {} got stale AppendEntriesEntry", me);
                    return;
                }
                if reply_term > term {
                    warn!(
                        "node {} got AppendEntriesEntry with newer term; becoming follower",
                        me
                    );
                    let mut raft = self_.raft.lock().await;
                    raft.become_follower(reply_term);
                    self_.start_election_timer(&mut raft);
                    return;
                }
                let mut raft = self_.raft.lock().await;
                if raft.append_entries_epoch > start_epoch {
                    warn!("node {}: append entries task got cancelled", me);
                    return;
                }
                if raft.state.term > term {
                    return;
                }
                if success {
                    raft.match_index[peer_index] = match_index;
                    raft.next_index[peer_index] = raft.match_index[peer_index] + 1;
                    debug!(
                        "node {}: updated peer state [{}]: match_index = {}, next_index = {}",
                        me, peer_index, raft.match_index[peer_index], raft.next_index[peer_index]
                    );
                    self_.advance_commit_index_leader(raft);
                } else if raft.next_index[peer_index] > 0 {
                    raft.next_index[peer_index] = raft.next_index[peer_index] - 1;
                    debug!(
                        "node {}: peer {} has stale log: match_index = {}, next_index = {}",
                        me, peer_index, raft.match_index[peer_index], raft.next_index[peer_index]
                    );
                    self_.send_log_entries_to(&raft, peer_index, peer_clone);
                }
            }
        });
    }

    fn advance_commit_index_leader(&self, mut raft: MutexGuard<Raft>) {
        let replicas_needed = raft.peers.len() / 2;
        let initial_commit_index = raft.commit_index;
        let mut n = raft.commit_index + 1;
        while n <= raft.log.len() {
            if raft.log[n - 1].term != raft.state.term {
                n += 1;
                continue;
            }
            let num_replicas = raft
                .match_index
                .iter()
                .filter(|match_index| **match_index >= n)
                .count();
            if num_replicas >= replicas_needed {
                debug!("node {} committing log entry {}", raft.me, n);
                raft.commit_index = n;
                n += 1;
            } else {
                debug!(
                    "node {}: log entry {} is on {}/{} replicas, not committing",
                    raft.me, n, num_replicas, replicas_needed
                );
                break;
            }
        }
        if raft.commit_index > initial_commit_index {
            raft.persist();
        }
        self.advance_state_machine(&mut raft);
    }

    fn advance_state_machine(&self, raft: &mut MutexGuard<Raft>) {
        while raft.last_applied < raft.commit_index {
            let n = raft.last_applied + 1;
            debug!("node {} applying log entry {}", raft.me, n);
            if raft
                .apply_ch
                .unbounded_send(ApplyMsg {
                    command_valid: true,
                    command: raft.log[n - 1].command.clone(),
                    command_index: n as u64,
                })
                .is_err()
            {
                warn!("node {}: apply_ch closed", raft.me);
            }
            raft.last_applied += 1;
        }
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        futures::executor::block_on(async move { (*self.raft.lock().await.state).clone() })
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        futures::executor::block_on(async move {
            let mut raft = self.raft.lock().await;
            raft.election_timer = None;
            raft.heartbeat_task = None;
        })
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    fn name(&self) -> &str {
        &self.name
    }

    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        let mut raft = self.raft.lock().await;
        let current_term = raft.state.term;
        let candidate = args.candidate_id as usize;
        if args.term < current_term {
            return Ok(RequestVoteReply {
                term: raft.state.term,
                vote_granted: false,
            });
        }
        if args.term > current_term {
            raft.become_follower(args.term);
            raft.vote = None;
        }

        let last_log_index = raft.log.len() as u64;
        let last_log_term = if last_log_index > 0 {
            raft.log[last_log_index as usize - 1].term
        } else {
            0
        } as u64;
        let vote_granted = match raft.vote {
            Some(other) if other != candidate => false,
            _ if args.last_log_term < last_log_term => false,
            _ if args.last_log_term == last_log_term && args.last_log_index < last_log_index => {
                false
            }
            _ => {
                raft.vote = Some(candidate);
                raft.persist();
                true
            }
        };
        Ok(RequestVoteReply {
            term: args.term,
            vote_granted,
        })
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        let mut raft = self.raft.lock().await;
        let current_term = raft.state.term;
        if args.term < current_term {
            return Ok(AppendEntriesReply {
                term: current_term,
                success: false,
            });
        }
        if args.term > current_term {
            raft.vote = None;
            raft.become_follower(args.term);
        }
        self.start_election_timer(&mut raft);

        if !raft.has_matching_log_entry(args.prev_log_index as usize, args.prev_log_term) {
            return Ok(AppendEntriesReply {
                term: args.term,
                success: false,
            });
        }

        for (i, entry) in args.entries.into_iter().enumerate() {
            let index = args.prev_log_index as usize + i;
            if index < raft.log.len() && raft.log[index].term != entry.term {
                debug!(
                    "node {} deleted stale log entries {}-{}",
                    self.me,
                    index + 1,
                    raft.log.len()
                );
                raft.log.truncate(index);
            }
            if index >= raft.log.len() {
                raft.log.push(entry);
            }
        }

        raft.commit_index = min(raft.log.len(), args.leader_commit as usize);

        raft.persist();

        self.advance_state_machine(&mut raft);

        Ok(AppendEntriesReply {
            term: args.term,
            success: true,
        })
    }
}
