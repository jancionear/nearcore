use crate::config::{NetworkTransitionConfig, PEERS_RESPONSE_MAX_PEERS};
use crate::network_protocol::testonly as data;
use crate::network_protocol::{
    Encoding, Handshake, HandshakeFailureReason, PartialEdgeInfo, PeerMessage, PeersRequest,
    PeersResponse, SyncAccountsData, T2MessageBody,
};
use crate::peer::peer_actor::ClosingReason;
use crate::peer::testonly::{PeerConfig, PeerHandle};
use crate::peer_manager::peer_manager_actor::Event;
use crate::tcp;
use crate::testonly::make_rng;
use crate::testonly::stream::Stream;
use crate::types::{
    Edge, PartialEncodedChunkRequestMsg, PartialEncodedChunkResponseMsg, ReasonForBan,
};
use assert_matches::assert_matches;
use near_async::{ActorSystem, time};
use near_o11y::testonly::init_test_logger;
use near_primitives::version::{MIN_SUPPORTED_PROTOCOL_VERSION, PROTOCOL_VERSION};
use std::sync::Arc;

#[allow(clippy::large_stack_frames)]
async fn test_peer_communication(
    outbound_transition: NetworkTransitionConfig,
    inbound_transition: NetworkTransitionConfig,
    want_encoding: Encoding,
) -> anyhow::Result<()> {
    tracing::info!(target: "test", ?outbound_transition, ?inbound_transition, "test_peer_communication");
    let mut rng = make_rng(89028037453);
    let mut clock = time::FakeClock::default();

    let chain = Arc::new(data::Chain::make(&mut clock, &mut rng, 12));
    let mut inbound_cfg = PeerConfig { chain: chain.clone(), network: chain.make_config(&mut rng) };
    inbound_cfg.network.transition = inbound_transition;
    let mut outbound_cfg =
        PeerConfig { chain: chain.clone(), network: chain.make_config(&mut rng) };
    outbound_cfg.network.transition = outbound_transition;
    let (outbound_stream, inbound_stream) =
        tcp::Stream::loopback(inbound_cfg.id(), tcp::Tier::T2).await;
    let actor_system = ActorSystem::new();
    let mut inbound = PeerHandle::start_endpoint(
        clock.clock(),
        actor_system.clone(),
        inbound_cfg,
        inbound_stream,
    );
    let mut outbound =
        PeerHandle::start_endpoint(clock.clock(), actor_system, outbound_cfg, outbound_stream);

    assert_eq!(outbound.complete_handshake().await, Some(want_encoding));
    assert_eq!(inbound.complete_handshake().await, Some(want_encoding));

    let message_processed = |want| {
        move |ev| match ev {
            Event::MessageProcessed(_, got) if got == want => Some(()),
            _ => None,
        }
    };

    tracing::info!(target:"test", "request update nonce");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::RequestUpdateNonce(PartialEdgeInfo::new(
        &outbound.cfg.network.node_id(),
        &inbound.cfg.network.node_id(),
        clock.now_utc().unix_timestamp() as u64,
        &outbound.cfg.network.node_key,
    ));
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!(target:"test", "peers request");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::PeersRequest(PeersRequest { max_peers: None, max_direct_peers: None });
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!(target:"test", "peers response");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::PeersResponse(PeersResponse {
        peers: (0..5).map(|_| data::make_peer_info(&mut rng)).collect(),
        direct_peers: vec![],
    });
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!(target:"test", "block request");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::BlockRequest(*chain.blocks[5].hash());
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!(target: "test", "block");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::Block(chain.blocks[5].clone());
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!(target:"test", "block headers request");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::BlockHeadersRequest(chain.blocks.iter().map(|b| *b.hash()).collect());
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!(target:"test", "block headers");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::BlockHeaders(chain.get_block_headers().map(Into::into).collect());
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!(target:"test", "sync routing table");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::SyncRoutingTable(data::make_routing_table(&mut rng));
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!(target:"test", "partial encoded chunk request");
    let mut events = inbound.events.from_now();
    let want = PeerMessage::Routed(Box::new(
        outbound.routed_message(
            T2MessageBody::PartialEncodedChunkRequest(PartialEncodedChunkRequestMsg {
                chunk_hash: chain.blocks[5].chunks()[2].chunk_hash().clone(),
                part_ords: vec![],
                tracking_shards: Default::default(),
            })
            .into(),
            inbound.cfg.id(),
            1, // ttl
            Some(clock.now_utc()),
        ),
    ));
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!(target:"test", "partial encoded chunk response");
    let mut events = inbound.events.from_now();
    let want_hash = chain.blocks[3].chunks()[0].chunk_hash().clone();
    let want_parts = data::make_chunk_parts(chain.chunks[&want_hash].clone());
    let want = PeerMessage::Routed(Box::new(
        outbound.routed_message(
            T2MessageBody::PartialEncodedChunkResponse(PartialEncodedChunkResponseMsg {
                chunk_hash: want_hash,
                parts: want_parts.clone(),
                receipts: vec![],
            })
            .into(),
            inbound.cfg.id(),
            1, // ttl
            Some(clock.now_utc()),
        ),
    ));
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!(target: "test", "transaction");
    let mut events = inbound.events.from_now();
    let want = data::make_signed_transaction(&mut rng);
    let want = PeerMessage::Transaction(want);
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    tracing::info!(target: "test", "sync accounts data");
    let mut events = inbound.events.from_now();
    let signed_encoding = outbound_transition.signing_encoding();
    let want = PeerMessage::SyncAccountsData(SyncAccountsData {
        accounts_data: (0..3)
            .map(|_| {
                Arc::new(data::make_signed_account_data(&mut rng, &clock.clock(), signed_encoding))
            })
            .collect(),
        incremental: true,
        requesting_full_sync: false,
    });
    outbound.send(want.clone()).await;
    events.recv_until(message_processed(want)).await;

    // TODO:
    // LastEdge, HandshakeFailure, Disconnect - affect the state of the PeerActor and are
    // observable only under specific conditions.
    Ok(())
}

#[tokio::test]
async fn peer_communication() -> anyhow::Result<()> {
    init_test_logger();
    use NetworkTransitionConfig as C;
    for (outbound, inbound, want_encoding) in [
        (C::CURRENT, C::CURRENT, Encoding::Borsh),
        (C::RELEASE_2_15, C::PROTOBUF_ONLY, Encoding::Proto),
        (C::PROTOBUF_ONLY, C::RELEASE_2_15, Encoding::Proto),
        (C::RELEASE_2_16, C::RELEASE_2_17, Encoding::Borsh),
    ] {
        test_peer_communication(outbound, inbound, want_encoding).await?;
    }
    Ok(())
}

/// Checks which releases can connect to each other and which encoding they agree on.
/// Releases next to each other have to be compatible.
#[tokio::test]
async fn encoding_negotiation() {
    init_test_logger();
    use NetworkTransitionConfig as C;
    let (p, v15, v16, v17) = (C::PROTOBUF_ONLY, C::RELEASE_2_15, C::RELEASE_2_16, C::RELEASE_2_17);
    let (proto, borsh) = (Some(Encoding::Proto), Some(Encoding::Borsh));
    let cases = [
        (p, p, proto),
        (p, v15, proto),
        (p, v16, None),
        (p, v17, None),
        (v15, p, proto),
        (v15, v15, borsh),
        (v15, v16, borsh),
        // 2.17 rejects the handshake of 2.15, which is signed using protobuf.
        (v15, v17, None),
        (v16, p, None),
        (v16, v15, borsh),
        (v16, v16, borsh),
        (v16, v17, borsh),
        (v17, p, None),
        (v17, v15, None),
        (v17, v16, borsh),
        (v17, v17, borsh),
    ];
    let mut rng = make_rng(89028037453);
    let mut clock = time::FakeClock::default();
    let chain = Arc::new(data::Chain::make(&mut clock, &mut rng, 12));
    for (outbound_transition, inbound_transition, want) in cases {
        tracing::info!(target: "test", ?outbound_transition, ?inbound_transition, "encoding_negotiation");
        let mut inbound_cfg =
            PeerConfig { chain: chain.clone(), network: chain.make_config(&mut rng) };
        inbound_cfg.network.transition = inbound_transition;
        let mut outbound_cfg =
            PeerConfig { chain: chain.clone(), network: chain.make_config(&mut rng) };
        outbound_cfg.network.transition = outbound_transition;
        if want.is_none() {
            // In some of the cases the peers never get a message they can decode.
            outbound_cfg.network.handshake_timeout = time::Duration::milliseconds(500);
        }
        let (outbound_stream, inbound_stream) =
            tcp::Stream::loopback(inbound_cfg.id(), tcp::Tier::T2).await;
        let actor_system = ActorSystem::new();
        let _inbound = PeerHandle::start_endpoint(
            clock.clock(),
            actor_system.clone(),
            inbound_cfg,
            inbound_stream,
        );
        let mut outbound =
            PeerHandle::start_endpoint(clock.clock(), actor_system, outbound_cfg, outbound_stream);
        let got = outbound
            .events
            .recv_until(|ev| match ev {
                Event::HandshakeCompleted(ev) => Some(ev.encoding),
                Event::ConnectionClosed(_) => Some(None),
                _ => None,
            })
            .await;
        assert_eq!(
            got, want,
            "outbound = {outbound_transition:?}, inbound = {inbound_transition:?}"
        );
    }
}

#[tokio::test]
async fn test_request_update_nonce_rejects_invalid_nonce() -> anyhow::Result<()> {
    init_test_logger();
    let mut rng = make_rng(89028037453);
    let mut clock = time::FakeClock::default();

    let chain = Arc::new(data::Chain::make(&mut clock, &mut rng, 12));
    let inbound_cfg = PeerConfig { chain: chain.clone(), network: chain.make_config(&mut rng) };
    let outbound_cfg = PeerConfig { chain: chain.clone(), network: chain.make_config(&mut rng) };
    let (outbound_stream, inbound_stream) =
        tcp::Stream::loopback(inbound_cfg.id(), tcp::Tier::T2).await;
    let actor_system = ActorSystem::new();
    let mut inbound = PeerHandle::start_endpoint(
        clock.clock(),
        actor_system.clone(),
        inbound_cfg,
        inbound_stream,
    );
    let mut outbound =
        PeerHandle::start_endpoint(clock.clock(), actor_system, outbound_cfg, outbound_stream);

    outbound.complete_handshake().await;
    inbound.complete_handshake().await;

    let mut events = inbound.events.from_now();
    let msg = PeerMessage::RequestUpdateNonce(PartialEdgeInfo::new(
        &outbound.cfg.network.node_id(),
        &inbound.cfg.network.node_id(),
        u64::MAX,
        &outbound.cfg.network.node_key,
    ));
    outbound.send(msg).await;

    events
        .recv_until(|ev| match ev {
            Event::ConnectionClosed(ev)
                if ev.reason == ClosingReason::Ban(ReasonForBan::InvalidEdge) =>
            {
                Some(())
            }
            _ => None,
        })
        .await;

    Ok(())
}

#[tokio::test]
async fn test_oversized_peers_response_bans_without_adding_to_peer_store() -> anyhow::Result<()> {
    init_test_logger();
    let mut rng = make_rng(89028037453);
    let mut clock = time::FakeClock::default();

    let chain = Arc::new(data::Chain::make(&mut clock, &mut rng, 12));
    let inbound_cfg = PeerConfig { chain: chain.clone(), network: chain.make_config(&mut rng) };
    let outbound_cfg = PeerConfig { chain: chain.clone(), network: chain.make_config(&mut rng) };
    let (outbound_stream, inbound_stream) =
        tcp::Stream::loopback(inbound_cfg.id(), tcp::Tier::T2).await;
    let actor_system = ActorSystem::new();
    let mut inbound = PeerHandle::start_endpoint(
        clock.clock(),
        actor_system.clone(),
        inbound_cfg,
        inbound_stream,
    );
    let mut outbound =
        PeerHandle::start_endpoint(clock.clock(), actor_system, outbound_cfg, outbound_stream);

    outbound.complete_handshake().await;
    inbound.complete_handshake().await;

    let store_size_before = inbound.network_state.peer_store.len();

    let fake_peers: Vec<_> =
        (0..PEERS_RESPONSE_MAX_PEERS + 1).map(|_| data::make_peer_info(&mut rng)).collect();
    let msg = PeerMessage::PeersResponse(PeersResponse { peers: fake_peers, direct_peers: vec![] });

    let mut events = inbound.events.from_now();
    outbound.send(msg).await;

    events
        .recv_until(|ev| match ev {
            Event::ConnectionClosed(ev)
                if ev.reason == ClosingReason::Ban(ReasonForBan::Abusive) =>
            {
                Some(())
            }
            _ => None,
        })
        .await;

    assert_eq!(
        inbound.network_state.peer_store.len(),
        store_size_before,
        "peer store should not be modified by an oversized PeersResponse"
    );
    Ok(())
}

// Verifies that HandshakeFailures are served correctly.
async fn test_handshake(encoding: Encoding) {
    let mut rng = make_rng(89028037453);
    let mut clock = time::FakeClock::default();

    let chain = Arc::new(data::Chain::make(&mut clock, &mut rng, 12));
    let inbound_cfg = PeerConfig { network: chain.make_config(&mut rng), chain: chain.clone() };
    let outbound_cfg = PeerConfig { network: chain.make_config(&mut rng), chain: chain.clone() };
    let (outbound_stream, inbound_stream) =
        tcp::Stream::loopback(inbound_cfg.id(), tcp::Tier::T2).await;
    let inbound =
        PeerHandle::start_endpoint(clock.clock(), ActorSystem::new(), inbound_cfg, inbound_stream);
    let outbound_port = outbound_stream.local_addr.port();
    let mut outbound = Stream::with_encoding(outbound_stream, encoding);

    // Send too old PROTOCOL_VERSION, expect ProtocolVersionMismatch
    let mut handshake = Handshake {
        protocol_version: MIN_SUPPORTED_PROTOCOL_VERSION - 1,
        oldest_supported_version: MIN_SUPPORTED_PROTOCOL_VERSION - 1,
        sender_peer_id: outbound_cfg.id(),
        target_peer_id: inbound.cfg.id(),
        sender_listen_port: Some(outbound_port),
        sender_chain_info: outbound_cfg.chain.get_peer_chain_info(),
        partial_edge_info: outbound_cfg
            .partial_edge_info(&inbound.cfg.id(), Edge::create_fresh_nonce(&clock.clock())),
        owned_account: None,
    };
    // We will also introduce chain_id mismatch, but ProtocolVersionMismatch is expected to take priority.
    handshake.sender_chain_info.genesis_id.chain_id = "unknown_chain".to_string();
    outbound.write(&PeerMessage::Tier2Handshake(handshake.clone())).await;
    let resp = outbound.read().await.unwrap();
    assert_matches!(
        resp,
        PeerMessage::HandshakeFailure(_, HandshakeFailureReason::ProtocolVersionMismatch { .. })
    );

    // Send too new PROTOCOL_VERSION, expect ProtocolVersionMismatch
    handshake.protocol_version = PROTOCOL_VERSION + 1;
    handshake.oldest_supported_version = PROTOCOL_VERSION + 1;
    outbound.write(&PeerMessage::Tier2Handshake(handshake.clone())).await;
    let resp = outbound.read().await.unwrap();
    assert_matches!(
        resp,
        PeerMessage::HandshakeFailure(_, HandshakeFailureReason::ProtocolVersionMismatch { .. })
    );

    // Send mismatching chain_id, expect GenesisMismatch.
    // We fix protocol_version, but chain_id is still mismatching.
    handshake.protocol_version = PROTOCOL_VERSION;
    handshake.oldest_supported_version = PROTOCOL_VERSION;
    outbound.write(&PeerMessage::Tier2Handshake(handshake.clone())).await;
    let resp = outbound.read().await.unwrap();
    assert_matches!(
        resp,
        PeerMessage::HandshakeFailure(_, HandshakeFailureReason::GenesisMismatch(_))
    );

    // Send a correct Handshake, expect a matching Handshake response.
    handshake.sender_chain_info = chain.get_peer_chain_info();
    outbound.write(&PeerMessage::Tier2Handshake(handshake.clone())).await;
    let (resp, resp_encoding) = outbound.read_with_encoding().await.unwrap();
    assert_matches!(resp, PeerMessage::Tier2Handshake(_));
    // The peer responds in the encoding of the handshake.
    assert_eq!(resp_encoding, encoding);
}

#[tokio::test]
async fn handshake() {
    init_test_logger();
    for encoding in [Encoding::Proto, Encoding::Borsh] {
        tracing::info!(target: "test", ?encoding, "test_handshake");
        test_handshake(encoding).await;
    }
}

/// A peer which sends the handshake in both encodings (like the outbound side does, until it
/// knows which encodings the other side supports) gets a single response, in borsh.
/// The protobuf copy of the handshake is ignored.
#[tokio::test]
async fn handshake_sent_in_both_encodings() {
    init_test_logger();
    let mut rng = make_rng(89028037453);
    let mut clock = time::FakeClock::default();

    let chain = Arc::new(data::Chain::make(&mut clock, &mut rng, 12));
    let inbound_cfg = PeerConfig { network: chain.make_config(&mut rng), chain: chain.clone() };
    let outbound_cfg = PeerConfig { network: chain.make_config(&mut rng), chain: chain.clone() };
    let (outbound_stream, inbound_stream) =
        tcp::Stream::loopback(inbound_cfg.id(), tcp::Tier::T2).await;
    let mut inbound =
        PeerHandle::start_endpoint(clock.clock(), ActorSystem::new(), inbound_cfg, inbound_stream);
    let outbound_port = outbound_stream.local_addr.port();
    let mut outbound = Stream::new(outbound_stream);

    let handshake = PeerMessage::Tier2Handshake(Handshake {
        protocol_version: PROTOCOL_VERSION,
        oldest_supported_version: PROTOCOL_VERSION,
        sender_peer_id: outbound_cfg.id(),
        target_peer_id: inbound.cfg.id(),
        sender_listen_port: Some(outbound_port),
        sender_chain_info: chain.get_peer_chain_info(),
        partial_edge_info: outbound_cfg
            .partial_edge_info(&inbound.cfg.id(), Edge::create_fresh_nonce(&clock.clock())),
        owned_account: None,
    });
    let mut events = inbound.events.from_now();
    outbound.write_encoded(&handshake.serialize(Encoding::Borsh)).await;
    outbound.write_encoded(&handshake.serialize(Encoding::Proto)).await;
    let (resp, encoding) = outbound.read_with_encoding().await.unwrap();
    assert_matches!(resp, PeerMessage::Tier2Handshake(_));
    assert_eq!(encoding, Encoding::Borsh);
    assert_eq!(inbound.complete_handshake().await, Some(Encoding::Borsh));

    // The connection stays open and keeps using borsh.
    let ping = PeerMessage::PeersRequest(PeersRequest { max_peers: None, max_direct_peers: None });
    outbound.write_encoded(&ping.serialize(Encoding::Borsh)).await;
    events
        .recv_until(|ev| match ev {
            Event::MessageProcessed(_, got) if got == ping => Some(()),
            Event::ConnectionClosed(ev) => panic!("connection closed: {}", ev.reason),
            _ => None,
        })
        .await;
}

/// Nothing but the first handshake, in both encodings, is sent before the encoding of the
/// connection is negotiated. In particular no `Disconnect` is sent when the handshake times out.
#[tokio::test]
async fn only_handshake_sent_before_negotiation() {
    init_test_logger();
    let mut rng = make_rng(89028037453);
    let mut clock = time::FakeClock::default();

    let chain = Arc::new(data::Chain::make(&mut clock, &mut rng, 12));
    let inbound_cfg = PeerConfig { network: chain.make_config(&mut rng), chain: chain.clone() };
    let mut outbound_cfg =
        PeerConfig { network: chain.make_config(&mut rng), chain: chain.clone() };
    outbound_cfg.network.handshake_timeout = time::Duration::milliseconds(500);
    let (outbound_stream, inbound_stream) =
        tcp::Stream::loopback(inbound_cfg.id(), tcp::Tier::T2).await;
    let _outbound = PeerHandle::start_endpoint(
        clock.clock(),
        ActorSystem::new(),
        outbound_cfg,
        outbound_stream,
    );
    let mut inbound = Stream::new(inbound_stream);

    let (handshake, encoding) = inbound.read_with_encoding().await.unwrap();
    assert_matches!(handshake, PeerMessage::Tier2Handshake(_));
    assert_eq!(encoding, Encoding::Borsh);
    assert_eq!(inbound.read_with_encoding().await.unwrap(), (handshake, Encoding::Proto));
    assert!(inbound.read_with_encoding().await.is_err());
}
