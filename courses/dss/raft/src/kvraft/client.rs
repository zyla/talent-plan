use std::fmt;
use std::cell::Cell;
use futures::executor::block_on;

use crate::proto::kvraftpb::*;

enum Op {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    pub servers: Vec<KvClient>,
    leader: Cell<usize>,
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<KvClient>) -> Clerk {
        Clerk { name, servers, leader: Cell::new(0), }
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // if let Some(reply) = self.servers[i].get(args).wait() { /* do something */ }
    pub fn get(&self, key: String) -> String {
        block_on(async move {
            let request = GetRequest { key };

            loop {
                match self.servers[self.leader.get()].get(&request).await {
                    Ok(GetReply { wrong_leader: false, value, .. }) => {
                        return value;
                    }
                    _ => {
                        self.next_leader();
                    }
                }
            }
        })
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    fn put_append(&self, op: Op) {
        block_on(async move {
            let request = match op {
                Op::Put(key, value) => {
                    PutAppendRequest { key, value, op: 1 }
                }
                Op::Append(key, value) => {
                    PutAppendRequest { key, value, op: 2 }
                }
            };

            loop {
                let server = self.leader.get();
                debug!("Trying {:?} on {}", request, server);
                match self.servers[server].put_append(&request).await {
                    Ok(PutAppendReply { wrong_leader: false, .. }) => {
                        debug!("{:?} success on {}", request, server);
                        return;
                    }
                    result => {
                        debug!("{:?} failed on {}: {:?}; trying next server", request, server, result);
                        self.next_leader();
                    }
                }
            }
        })
    }

    fn next_leader(&self) {
        self.leader.set((self.leader.get() + 1) % self.servers.len());
    }

    pub fn put(&self, key: String, value: String) {
        self.put_append(Op::Put(key, value))
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(Op::Append(key, value))
    }
}
