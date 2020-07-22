use rand::Rng;
use std::sync::mpsc::{sync_channel, Receiver};
use std::sync::Arc;

use futures::channel::mpsc::UnboundedSender;
use futures::executor::ThreadPool;
use futures::lock::Mutex;
use futures::Future;
use futures::future::FutureExt;
use futures::task::SpawnExt;

use futures_timer::Delay;
use std::time::Duration;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

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
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    apply_ch: UnboundedSender<ApplyMsg>,
    election_timer: Option<CancellableTask>,
}

struct CancellableTask {
    sender: futures::channel::oneshot::Sender<()>,
}

impl CancellableTask {
    fn spawn<F: Future + std::marker::Unpin + std::marker::Send + 'static>(executor: impl futures::task::Spawn, task: F) -> Self {
        let (sender, receiver) = futures::channel::oneshot::channel();
        executor.spawn(async move {
            futures::future::select(receiver, task).await;
        }).unwrap();
        CancellableTask { sender }
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
            apply_ch,
            election_timer: None,
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
        let mut node = Node {
            me: raft.me,
            raft: Arc::new(Mutex::new(raft)),
            pool: Arc::new(ThreadPool::new().unwrap()),
        };
        node.start_election_timer();
        node
    }

    fn start_election_timer(&mut self) {
        let me = self.me;
        let raft = self.raft.clone();
        let raft2 = self.raft.clone();
        let pool = self.pool.clone();
        let pool2 = self.pool.clone();
        self.pool.spawn_ok(async move {
            let mut raft = raft.lock().await;
            raft.election_timer = Some(CancellableTask::spawn(&pool, {
                let timeout = rand::thread_rng().gen_range(100, 300);
                Delay::new(Duration::from_millis(timeout)).map(move |_| {
                    println!("node {}: election timeout ({})", me, timeout);
                    pool2.spawn_ok(async move {
                        println!("node {}: starting election", me);
                        let mut raft = raft2.lock().await;
                        let term = raft.state.term;
                        for (index, peer) in raft.peers.iter().enumerate() {
                            if index != me {
                                let peer_clone = peer.clone();
                                peer.spawn(async move {
                                    peer_clone.request_vote(&crate::proto::raftpb::RequestVoteArgs{ candidate_id: me as u64, term }).await;
                                });
                            }
                        };
                    });
                })
            }));
        });
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
        Ok(RequestVoteReply { term: args.term, vote_granted: true })
    }
}
