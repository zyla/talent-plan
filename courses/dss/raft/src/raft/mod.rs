use rand::Rng;

use std::sync::Arc;

use futures::channel::mpsc::UnboundedSender;
use futures::executor::ThreadPool;

use futures::lock::{Mutex, MutexGuard};
use futures::Future;

use futures_timer::Delay;
use std::time::Duration;

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
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    apply_ch: UnboundedSender<ApplyMsg>,
    election_timer: Option<CancellableTask>,
    heartbeat_task: Option<CancellableTask>,
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
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn modify_state(&mut self, f: impl Fn(&mut State) -> ()) {
        let mut state = (*self.state).clone();
        f(&mut state);
        self.state = Arc::new(state);
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        self.persist();
        let _ = &self.state;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
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
    raft: Arc<Mutex<Raft>>,
    pool: Arc<ThreadPool>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        let node = Node {
            me: raft.me,
            raft: Arc::new(Mutex::new(raft)),
            pool: Arc::new(ThreadPool::new().unwrap()),
        };
        let node_ = node.clone();
        node.pool.spawn_ok(async move {
            node_.start_election_timer(node_.clone().raft.lock().await);
        });
        node
    }

    fn start_election_timer(&self, mut raft: MutexGuard<Raft>) {
        let self_ = self.clone();
        let me = self_.me;
        raft.election_timer = Some(CancellableTask::spawn(&self.pool, async move {
            let timeout = rand::thread_rng().gen_range(100, 300);
            Delay::new(Duration::from_millis(timeout)).await;
            println!("node {}: election timeout ({})", me, timeout);
            {
                let mut raft = self_.raft.lock().await;
                raft.modify_state(|state| {
                    state.is_leader = false;
                    state.term += 1;
                });
                let term = raft.state.term;
                let peers = raft.peers.clone();
                let votes_needed = peers.len() / 2;
                println!(
                    "node {}: starting election, waiting for {} votes",
                    me, votes_needed
                );
                drop(raft);

                let wg = Arc::new(WaitGroup::new(votes_needed));
                for (index, peer) in peers.iter().enumerate() {
                    if index != me {
                        let peer_clone = peer.clone();
                        let wg = wg.clone();
                        peer.spawn(async move {
                            match peer_clone
                                .request_vote(&RequestVoteArgs {
                                    candidate_id: me as u64,
                                    term,
                                })
                                .await
                            {
                                Ok(RequestVoteReply {
                                    vote_granted: true, ..
                                }) => {
                                    wg.done();
                                }
                                _ => {}
                            }
                        });
                    }
                }
                wg.wait().await;
                println!("node {}: got votes, becoming a leader", me);

                self_.run_leader(term).await;
            }
        }));
    }

    async fn run_leader(&self, term: u64) {
        let me = self.me;
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

        let _self_ = self.clone();

        let peers = raft.peers.clone();
        raft.heartbeat_task = Some(CancellableTask::spawn(&self.pool, async move {
            loop {
                println!("node {}: sending heartbeats", me);
                for (index, peer) in peers.iter().enumerate() {
                    if index != me {
                        let peer_clone = peer.clone();
                        peer.spawn(async move {
                            match peer_clone.append_entries(&AppendEntriesArgs { term }).await {
                                Ok(AppendEntriesReply { .. }) => {}
                                _ => {}
                            }
                        });
                    }
                }
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
    pub fn start<M>(&self, _command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        futures::executor::block_on(async move {
            let raft = self.raft.lock().await;
            let _ = raft.apply_ch;
            if raft.state.is_leader {
                unimplemented!()
            } else {
                // TODO: What to do when election is in progress?
                Err(Error::NotLeader)
            }
        })
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
        // Your code here, if desired.
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        println!("node {} got {:?}", self.me, args);
        let mut raft = self.raft.lock().await;
        let candidate = args.candidate_id as usize;
        let response = if args.term < raft.state.term {
            Ok(RequestVoteReply {
                term: raft.state.term,
                vote_granted: false,
            })
        } else if args.term == raft.state.term {
            let vote_granted = match raft.vote {
                Some(other) if other != candidate => false,
                _ => {
                    raft.vote = Some(candidate);
                    true
                }
            };
            Ok(RequestVoteReply {
                term: args.term,
                vote_granted,
            })
        } else {
            raft.modify_state(|state| {
                state.is_leader = false;
                state.term = args.term;
            });
            raft.vote = Some(candidate);
            Ok(RequestVoteReply {
                term: args.term,
                vote_granted: true,
            })
        };
        println!("node {} responds with {:?}", self.me, response);
        response
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        println!("node {} got {:?}", self.me, args);
        let mut raft = self.raft.lock().await;
        let response = if args.term < raft.state.term {
            Ok(AppendEntriesReply {
                term: raft.state.term,
            })
        } else if args.term == raft.state.term {
            raft.election_timer = None;
            Ok(AppendEntriesReply { term: args.term })
        } else {
            raft.election_timer = None;
            raft.modify_state(|state| {
                state.is_leader = false;
                state.term = args.term;
            });
            Ok(AppendEntriesReply { term: args.term })
        };
        println!("node {} responds with {:?}", self.me, response);
        response
    }
}
