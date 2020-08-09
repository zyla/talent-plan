#![deny(clippy::all)]

#[allow(unused_imports)]
#[macro_use]
extern crate log;
#[allow(unused_imports)]
#[macro_use]
extern crate prost_derive;

#[allow(unused_imports)]
#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate futures;

pub mod kvraft;
mod proto;
pub mod raft;
