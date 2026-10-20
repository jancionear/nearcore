use crate::auto_stop::AutoStopActor;
use crate::concurrency::outgoing_queue_limiter::OutgoingPermit;
use crate::config::DEFAULT_MAX_WRITE_BUFFER_CAPACITY_BYTES;
use crate::network_protocol::testonly as data;
use crate::network_protocol::{
    Encoding, PeerMessage, PeerMessageFrames, PeerMessageKind, PeersRequest,
};
use crate::peer::stream::{
    self, IncomingFrame, MAX_FIRST_FRAME_SIZE_BYTES, MAX_HEADER_SIZE_BYTES, ProtocolViolation,
    RecvError,
};
use crate::peer_manager::network_state::INCOMING_SEMAPHORE_PERMITS;
use crate::tcp;
use crate::testonly::{Rng, make_rng};
use near_async::messaging::{CanSendAsync, IntoAsyncSender, IntoSender};
use near_async::time;
use near_async::tokio::TokioRuntimeHandle;
use near_async::{ActorSystem, messaging};
use near_primitives::hash::CryptoHash;
use rand::Rng as _;
use std::sync::Arc;
use tokio::sync::{Semaphore, mpsc};

struct Actor {
    handle: TokioRuntimeHandle<Self>,
    stream: stream::FramedStream,
    queue_send: mpsc::UnboundedSender<IncomingFrame>,
    error_send: mpsc::UnboundedSender<stream::Error>,
}

impl messaging::Actor for Actor {}

#[derive(Debug)]
struct SendFrame(PeerMessageFrames);

impl messaging::Handler<SendFrame> for Actor {
    fn handle(&mut self, SendFrame(message): SendFrame) {
        self.stream.send(stream::Frame::with_permit(message, OutgoingPermit::fake_for_test()));
    }
}

impl messaging::Handler<IncomingFrame> for Actor {
    fn handle(&mut self, frame: IncomingFrame) {
        self.queue_send.send(frame).ok().unwrap();
    }
}

impl messaging::Handler<stream::Error> for Actor {
    fn handle(&mut self, err: stream::Error) {
        self.error_send.send(err).ok().unwrap();
        self.handle.stop();
    }
}

struct Handler {
    queue_recv: mpsc::UnboundedReceiver<IncomingFrame>,
    error_recv: mpsc::UnboundedReceiver<stream::Error>,
    system: AutoStopActor<Actor>,
}

impl Handler {
    async fn send(&self, message: PeerMessageFrames) {
        self.system.send_async(SendFrame(message)).await.unwrap();
    }

    /// Waits for the next message. Returns `None` once the stream is closed.
    async fn recv(&mut self) -> Option<PeerMessageFrames> {
        self.queue_recv.recv().await.map(|frame| frame.message)
    }
}

impl Actor {
    fn spawn(
        actor_system: ActorSystem,
        s: tcp::Stream,
        accepted_encodings: &'static [Encoding],
    ) -> Handler {
        let (queue_send, queue_recv) = mpsc::unbounded_channel();
        let (error_send, error_recv) = mpsc::unbounded_channel();
        let builder = actor_system.new_tokio_builder();
        let handle = builder.handle();
        let framed_stream = stream::FramedStream::spawn(
            handle.clone().into_sender(),
            handle.clone().into_async_sender(),
            &*handle.future_spawner(),
            s,
            Arc::default(),
            Arc::new(Semaphore::new(INCOMING_SEMAPHORE_PERMITS)),
            accepted_encodings,
            DEFAULT_MAX_WRITE_BUFFER_CAPACITY_BYTES,
        );
        let actor = Actor { handle: handle.clone(), stream: framed_stream, queue_send, error_send };
        builder.spawn_tokio_actor(actor);
        Handler { queue_recv, error_recv, system: AutoStopActor(handle) }
    }
}

const BORSH_AND_PROTO: &[Encoding] = &[Encoding::Borsh, Encoding::Proto];

/// Returns a stream which sends messages, and a stream which receives them, accepting
/// `accepted_encodings`.
async fn connect(rng: &mut Rng, accepted_encodings: &'static [Encoding]) -> (Handler, Handler) {
    let (s1, s2) = tcp::Stream::loopback(data::make_peer_id(rng), tcp::Tier::T2).await;
    let actor_system = ActorSystem::new();
    let sender = Actor::spawn(actor_system.clone(), s1, BORSH_AND_PROTO);
    let receiver = Actor::spawn(actor_system, s2, accepted_encodings);
    (sender, receiver)
}

const BLOCK_REQUEST: PeerMessageKind = PeerMessageKind::BlockRequest;

/// A borsh-encoded message with a valid header and the given body.
fn borsh_message(body: Vec<u8>) -> PeerMessageFrames {
    let header = PeerMessage::BlockRequest(CryptoHash::default()).serialize(Encoding::Borsh).header;
    PeerMessageFrames { header, body }
}

fn random_body(rng: &mut Rng) -> Vec<u8> {
    let mut body = vec![0; rng.gen_range(0..10000)];
    rng.fill(&mut body[..]);
    body
}

#[tokio::test]
async fn send_recv_proto() {
    let mut rng = make_rng(98324532);
    let (sender, mut receiver) = connect(&mut rng, BORSH_AND_PROTO).await;
    let msgs: Vec<_> = (0..20)
        .map(|_| {
            let mut body = random_body(&mut rng);
            // A protobuf-encoded message never starts with a byte smaller than 8, so it is
            // never a valid header.
            if let Some(first) = body.first_mut() {
                *first |= 8;
            }
            PeerMessageFrames { header: None, body }
        })
        .collect();
    for msg in &msgs {
        sender.send(msg.clone()).await;
    }
    for want in msgs {
        assert_eq!(receiver.recv().await, Some(want));
    }
}

#[tokio::test]
async fn send_recv_borsh() {
    let mut rng = make_rng(98324532);
    let (sender, mut receiver) = connect(&mut rng, BORSH_AND_PROTO).await;
    let msgs: Vec<_> = (0..20).map(|_| borsh_message(random_body(&mut rng))).collect();
    for msg in &msgs {
        sender.send(msg.clone()).await;
    }
    for want in msgs {
        assert_eq!(receiver.recv().await, Some(want));
    }
}

/// The protobuf copy of the first handshake, which follows the borsh-encoded one, is skipped.
#[tokio::test]
async fn skip_protobuf_handshake() {
    let mut rng = make_rng(98324532);
    let mut clock = time::FakeClock::default();
    let chain = data::Chain::make(&mut clock, &mut rng, 1);
    let handshake = PeerMessage::Tier2Handshake(data::make_handshake(&mut rng, &chain));
    let (sender, mut receiver) = connect(&mut rng, BORSH_AND_PROTO).await;
    let next = borsh_message(random_body(&mut rng));
    sender.send(handshake.serialize(Encoding::Borsh)).await;
    sender.send(handshake.serialize(Encoding::Proto)).await;
    sender.send(next.clone()).await;
    assert_eq!(receiver.recv().await, Some(handshake.serialize(Encoding::Borsh)));
    assert_eq!(receiver.recv().await, Some(next));
}

/// A node which doesn't accept borsh receives the frames of a borsh-encoded message as
/// protobuf-encoded messages (which are invalid), like the nodes which support only protobuf.
#[tokio::test]
async fn protobuf_only() {
    let mut rng = make_rng(98324532);
    let (sender, mut receiver) = connect(&mut rng, &[Encoding::Proto]).await;
    let msg = borsh_message(random_body(&mut rng));
    sender.send(msg.clone()).await;
    assert_eq!(
        receiver.recv().await,
        Some(PeerMessageFrames { header: None, body: msg.header.unwrap() })
    );
    assert_eq!(receiver.recv().await, Some(PeerMessageFrames { header: None, body: msg.body }));
}

/// Anything which doesn't follow the encoding of the stream closes it.
#[tokio::test]
async fn protocol_violations() {
    let mut rng = make_rng(98324532);
    let mut clock = time::FakeClock::default();
    let chain = data::Chain::make(&mut clock, &mut rng, 1);
    let handshake = PeerMessage::Tier2Handshake(data::make_handshake(&mut rng, &chain));
    let proto_handshake = handshake.serialize(Encoding::Proto);
    let proto_message =
        PeerMessage::PeersRequest(PeersRequest { max_peers: Some(1), max_direct_peers: None })
            .serialize(Encoding::Proto);
    let valid = borsh_message(vec![1; 100]);
    let mut too_large_header = valid.header.clone().unwrap();
    too_large_header.resize(MAX_HEADER_SIZE_BYTES + 1, 0);

    // Encodings accepted by the receiver, the sent messages, the messages received before the
    // stream is closed and the violation which closes it.
    type Case = (
        &'static [Encoding],
        Vec<PeerMessageFrames>,
        Vec<PeerMessageFrames>,
        fn(&ProtocolViolation) -> bool,
    );
    let cases: Vec<Case> = vec![
        // Protobuf, when it is not accepted.
        (&[Encoding::Borsh], vec![proto_handshake.clone()], vec![], |v| {
            matches!(v, ProtocolViolation::ProtobufNotAllowed)
        }),
        // A first frame which is too large to be a handshake.
        (
            BORSH_AND_PROTO,
            vec![PeerMessageFrames { header: None, body: vec![8; MAX_FIRST_FRAME_SIZE_BYTES + 1] }],
            vec![],
            |v| matches!(v, ProtocolViolation::FrameTooLarge { .. }),
        ),
        // A protobuf-encoded message other than the copy of the first handshake.
        (BORSH_AND_PROTO, vec![valid.clone(), proto_message], vec![valid.clone()], |v| {
            matches!(v, ProtocolViolation::InvalidHeader(_))
        }),
        // A protobuf-encoded handshake, other than right after the first message.
        (
            BORSH_AND_PROTO,
            vec![valid.clone(), proto_handshake.clone(), valid.clone(), proto_handshake],
            vec![valid.clone(), valid.clone()],
            |v| matches!(v, ProtocolViolation::InvalidHeader(_)),
        ),
        // An invalid header.
        (
            BORSH_AND_PROTO,
            vec![
                valid.clone(),
                valid.clone(),
                PeerMessageFrames { header: Some(vec![1]), body: vec![] },
            ],
            vec![valid.clone(), valid.clone()],
            |v| matches!(v, ProtocolViolation::InvalidHeader(_)),
        ),
        // A header which is too large.
        (
            BORSH_AND_PROTO,
            vec![
                valid.clone(),
                valid.clone(),
                PeerMessageFrames { header: Some(too_large_header), body: vec![] },
            ],
            vec![valid.clone(), valid.clone()],
            |v| matches!(v, ProtocolViolation::FrameTooLarge { .. }),
        ),
        // A body which exceeds the size limit of the message kind.
        (
            BORSH_AND_PROTO,
            vec![borsh_message(vec![0; BLOCK_REQUEST.max_size() + 1])],
            vec![],
            |v| matches!(v, ProtocolViolation::BodyTooLarge { kind: BLOCK_REQUEST, .. }),
        ),
    ];
    for (i, (accepted_encodings, sent, received, want_violation)) in cases.into_iter().enumerate() {
        let (sender, mut receiver) = connect(&mut rng, accepted_encodings).await;
        for msg in sent {
            sender.send(msg).await;
        }
        for want in received {
            assert_eq!(receiver.recv().await, Some(want), "case {i}");
        }
        assert_eq!(receiver.recv().await, None, "case {i}");
        match receiver.error_recv.recv().await {
            Some(stream::Error::Recv(RecvError::ProtocolViolation(violation))) => {
                assert!(want_violation(&violation), "case {i}: {violation}");
            }
            err => panic!("case {i}: expected a protocol violation, got {err:?}"),
        }
    }
}
