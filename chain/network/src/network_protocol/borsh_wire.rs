//! Borsh wire format of the network protocol.
//!
//! WARNING: these types define the protocol, all changes need to maintain backward compatibility.
//!
//! A borsh-encoded `PeerMessage` (the payload of a single length-prefixed frame) consists of
//! a `PeerMessageHeader`, which tells the receiver how to handle the message before it decodes
//! the body, followed by the `PeerMessage` itself.
//!
//! The first byte is the version of the header, which is smaller than 8, so it is never a valid
//! protobuf tag and a borsh-encoded message cannot be confused with a protobuf-encoded one, see
//! `PeerMessageFrames`.
use crate::network_protocol::PeerMessageKind;
use borsh::{BorshDeserialize, BorshSerialize};

/// Header of a borsh-encoded message. Versioned, so that it can be extended in the future.
/// Discriminants have to stay smaller than 8, see `PeerMessageFrames`.
#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq, Eq)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub(super) enum PeerMessageHeader {
    V1(PeerMessageHeaderV1) = 0,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq, Eq)]
pub(super) struct PeerMessageHeaderV1 {
    /// Kind of the message in the body. The receiver checks it against the decoded body.
    pub kind: PeerMessageKind,
}
