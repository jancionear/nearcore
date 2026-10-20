//! Borsh encoding of the network protocol types, see `borsh_wire`.
use super::borsh_wire::*;
use super::{
    MAX_ROUTED_MESSAGE_SIZE_BYTES, MAX_SHARDS_PER_SNAPSHOT_HOST_INFO, MAX_TRACKED_SHARDS_PER_PEER,
    MAX_TRANSACTION_SIZE_BYTES, ParsePeerMessageError, PeerMessage, PeerMessageFrames,
    PeerMessageKind,
};
use borsh::BorshDeserialize as _;
use std::io;

#[derive(thiserror::Error, Debug)]
pub enum ParseBorshPeerMessageError {
    #[error("header: {0}")]
    Header(#[source] io::Error),
    #[error("body: {0}")]
    Body(#[source] io::Error),
    #[error("header kind {header:?} doesn't match the body kind {body:?}")]
    KindMismatch { header: PeerMessageKind, body: PeerMessageKind },
    #[error("handshake: tracked_shards: {count} > {max} (MAX_TRACKED_SHARDS_PER_PEER)")]
    TooManyTrackedShards { count: usize, max: usize },
    #[error("sync_snapshot_hosts: shards: {count} > {max} (MAX_SHARDS_PER_SNAPSHOT_HOST_INFO)")]
    TooManySnapshotHostShards { count: usize, max: usize },
}

/// Serializes a message into its header and body frames.
pub(super) fn serialize_peer_message(msg: &PeerMessage) -> PeerMessageFrames {
    let header = PeerMessageHeader::V1(PeerMessageHeaderV1 { kind: msg.kind() });
    PeerMessageFrames {
        header: Some(borsh::to_vec(&header).unwrap()),
        body: borsh::to_vec(msg).unwrap(),
    }
}

/// Parses the header of a borsh-encoded message. Returns the kind of the message.
pub(crate) fn parse_borsh_header(
    header: &[u8],
) -> Result<PeerMessageKind, ParseBorshPeerMessageError> {
    let header =
        PeerMessageHeader::try_from_slice(header).map_err(ParseBorshPeerMessageError::Header)?;
    Ok(match header {
        PeerMessageHeader::V1(header) => header.kind,
    })
}

/// Maximum size of the body of a borsh-encoded message of the given kind.
pub(crate) fn max_borsh_body_size(kind: PeerMessageKind) -> usize {
    // The protobuf encoding applies the transaction and routed message limits to the
    // borsh-encoded transaction and routed message it carries. Apply the same limits here, so
    // that both encodings accept the same messages.
    match kind {
        PeerMessageKind::Transaction => kind.max_size().min(MAX_TRANSACTION_SIZE_BYTES),
        PeerMessageKind::Routed(_) => kind.max_size().min(MAX_ROUTED_MESSAGE_SIZE_BYTES),
        _ => kind.max_size(),
    }
}

/// Deserializes a message from its header and body frames.
pub(super) fn deserialize_peer_message(
    header: &[u8],
    body: &[u8],
) -> Result<PeerMessage, ParsePeerMessageError> {
    let kind = parse_borsh_header(header)?;
    let max_size = max_borsh_body_size(kind);
    if body.len() > max_size {
        return Err(ParsePeerMessageError::TooLarge { kind, size: body.len(), max_size });
    }
    let msg = PeerMessage::try_from_slice(body).map_err(ParseBorshPeerMessageError::Body)?;
    if msg.kind() != kind {
        return Err(
            ParseBorshPeerMessageError::KindMismatch { header: kind, body: msg.kind() }.into()
        );
    }
    check_limits(&msg)?;
    Ok(msg)
}

/// Checks the limits which the protobuf decoding applies to the decoded message.
fn check_limits(msg: &PeerMessage) -> Result<(), ParseBorshPeerMessageError> {
    match msg {
        PeerMessage::Tier1Handshake(handshake)
        | PeerMessage::Tier2Handshake(handshake)
        | PeerMessage::Tier3Handshake(handshake) => {
            let count = handshake.sender_chain_info.tracked_shards.len();
            if count > MAX_TRACKED_SHARDS_PER_PEER {
                return Err(ParseBorshPeerMessageError::TooManyTrackedShards {
                    count,
                    max: MAX_TRACKED_SHARDS_PER_PEER,
                });
            }
        }
        PeerMessage::SyncSnapshotHosts(ssh) => {
            if let Some(host) =
                ssh.hosts.iter().find(|host| host.shards.len() > MAX_SHARDS_PER_SNAPSHOT_HOST_INFO)
            {
                return Err(ParseBorshPeerMessageError::TooManySnapshotHostShards {
                    count: host.shards.len(),
                    max: MAX_SHARDS_PER_SNAPSHOT_HOST_INFO,
                });
            }
        }
        PeerMessage::HandshakeFailure(_, _)
        | PeerMessage::LastEdge(_)
        | PeerMessage::SyncRoutingTable(_)
        | PeerMessage::RequestUpdateNonce(_)
        | PeerMessage::SyncAccountsData(_)
        | PeerMessage::PeersRequest(_)
        | PeerMessage::PeersResponse(_)
        | PeerMessage::BlockHeadersRequest(_)
        | PeerMessage::BlockHeaders(_)
        | PeerMessage::BlockRequest(_)
        | PeerMessage::Block(_)
        | PeerMessage::OptimisticBlock(_)
        | PeerMessage::Transaction(_)
        | PeerMessage::Routed(_)
        | PeerMessage::Disconnect(_)
        | PeerMessage::Challenge(_)
        | PeerMessage::VersionedStateResponse(_)
        | PeerMessage::EpochSyncRequest
        | PeerMessage::EpochSyncResponse(_) => {}
    }
    Ok(())
}
