use std::sync::Arc;
use futures::StreamExt;
use futures::channel::mpsc::unbounded;
use futures::channel::mpsc::UnboundedReceiver;
use futures::channel::oneshot;
use futures::lock::Mutex;
use std::collections::HashMap;

use crate::proto::kvraftpb::*;
use crate::raft;

pub struct KvServer {
    pub rf: raft::Node,
    me: usize,
    // snapshot if log grows this big
    maxraftstate: Option<usize>,

    apply_ch: std::sync::Mutex<Option<UnboundedReceiver<raft::ApplyMsg>>>,
}

impl KvServer {
    pub fn new(
        servers: Vec<crate::proto::raftpb::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        // You may need initialization code here.

        let (tx, apply_ch) = unbounded();
        let rf = raft::Node::new(raft::Raft::new(servers, me, persister, tx));

        KvServer {
            rf,
            me,
            maxraftstate,
            apply_ch: std::sync::Mutex::new(Some(apply_ch)),
        }
    }
}

impl KvServer {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = &self.me;
        let _ = &self.maxraftstate;
        let _ = Reply::GetReply { value: "".into() };
        let _ = Reply::PutAppendReply;
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Get {
        key: String,
    },
    PutAppend {
        key: String,
        value: String,
        op: Op,
    },
}

#[derive(Debug)]
enum Reply {
    GetReply { value: String },
    PutAppendReply,
}

// Choose concurrency paradigm.
//
// You can either drive the kv server by the rpc framework,
//
// ```rust
// struct Node { server: Arc<Mutex<KvServer>> }
// ```
//
// or spawn a new thread runs the kv server and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    server: Arc<KvServer>,
    receivers: Arc<Mutex<HashMap<u64, oneshot::Sender<Reply>>>>,
    store: Arc<Mutex<HashMap<String, String>>>,
}

impl Node {
    pub fn new(kv: KvServer) -> Node {
        let node = Node {
            server: Arc::new(kv),
            receivers: Arc::new(Mutex::new(Default::default())),
            store: Arc::new(Mutex::new(Default::default())),
        };

        let node_clone = node.clone();
        let node_clone_2 = node.clone();

        async_std::task::spawn(node_clone_2.server.apply_ch.lock().unwrap().take().unwrap().for_each(move |cmd: raft::ApplyMsg| {
            let node2 = node.clone();
            async move {
                if !cmd.command_valid {
                    // ignore other types of ApplyMsg
                    return;
                }
                match serde_cbor::from_slice(&cmd.command) {
                    Ok(command) => {
                        node2.apply(cmd.command_index, command).await;
                    }
                    Err(e) => {
                        panic!("committed command is not an entry {:?}", e);
                    }
                }
            }
        }));

        node_clone
    }

    async fn apply(&self, command_index: u64, command: Command) {
        debug!("node {}: apply {} {:?}", self.server.me, command_index, command);
        let reply = match command {
            Command::Get { key } => {
                let value = self.store.lock().await.get(&key).map(|s| s.clone()).unwrap_or_else(|| "".into());
                Reply::GetReply { value }
            },
            Command::PutAppend { key, value, op: Op::Put } => {
                self.store.lock().await.insert(key, value);
                Reply::PutAppendReply
            },
            Command::PutAppend { key, value, op: Op::Append } => {
                self.store.lock().await.entry(key).or_insert_with(|| "".into()).push_str(&value);
                Reply::PutAppendReply
            },
            Command::PutAppend { op: Op::Unknown, .. } => {
                panic!("Unknown op");
            },
        };
        let mut receivers = self.receivers.lock().await;
        let listener = receivers.remove(&command_index);
        debug!("node {}: apply {} result: {:?} (has_listener={:?})", self.server.me, command_index, reply, listener.is_some());
        if let Some(tx) = listener {
            tx.send(reply).unwrap();
        }
    }

    /// the tester calls kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // If you want to free some resources by `raft::Node::kill` method,
        // you should call `raft::Node::kill` here also to prevent resource leaking.
        // Since the test framework will call kvraft::Node::kill only.
        // self.server.kill();

        // Your code here, if desired.
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    pub fn get_state(&self) -> raft::State {
        self.server.rf.get_state()
    }
}

#[async_trait::async_trait]
impl KvService for Node {
    async fn get(&self, arg: GetRequest) -> labrpc::Result<GetReply> {
        let encoded_command = serde_cbor::to_vec(&Command::Get { key: arg.key }).expect("command should encode without problems");
        let mut receivers = self.receivers.lock().await;
        match self.server.rf.do_start(encoded_command).await {
            Ok((index, _)) => {
                let (tx, rx) = oneshot::channel();
                receivers.insert(index, tx);
                drop(receivers);
                match rx.await {
                    Ok(Reply::GetReply { value }) => {
                        Ok(GetReply {
                            wrong_leader: false,
                            err: "".into(),
                            value,
                        })
                    },
                    _ => {
                        Ok(GetReply {
                            wrong_leader: true,
                            err: "Not leader".into(),
                            value: "".into(),
                        })
                    },
                }
            }
            Err(raft::errors::Error::NotLeader) => {
                Ok(GetReply {
                    wrong_leader: true,
                    err: "Not leader".into(),
                    value: "".into(),
                })
            }
            Err(err) => {
                panic!("Unexpected error: {:?}", err);
            }
        }
    }

    async fn put_append(&self, mut arg: PutAppendRequest) -> labrpc::Result<PutAppendReply> {
        let encoded_command = serde_cbor::to_vec(&Command::PutAppend {
            key: std::mem::replace(&mut arg.key, "".into()),
            value: std::mem::replace(&mut arg.value, "".into()),
            op: arg.op()
        }).expect("command should encode without problems");
        let mut receivers = self.receivers.lock().await;
        match self.server.rf.do_start(encoded_command).await {
            Ok((index, _)) => {
                let (tx, rx) = oneshot::channel();
                receivers.insert(index, tx);
                drop(receivers);
                match rx.await {
                    Ok(Reply::PutAppendReply) => {
                        Ok(PutAppendReply {
                            wrong_leader: false,
                            err: "".into(),
                        })
                    },
                    _ => {
                        Ok(PutAppendReply {
                            wrong_leader: true,
                            err: "Not leader".into(),
                        })
                    },
                }
            }
            Err(raft::errors::Error::NotLeader) => {
                Ok(PutAppendReply {
                    wrong_leader: true,
                    err: "Not leader".into(),
                })
            }
            Err(err) => {
                panic!("Unexpected error: {:?}", err);
            }
        }
    }
}
