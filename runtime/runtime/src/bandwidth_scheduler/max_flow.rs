use near_primitives::bandwidth_scheduler::Bandwidth;
use near_primitives::types::ShardId;
use std::collections::BTreeMap;

#[allow(unused)]
pub fn theoretical_max_flow(
    outgoing_limits: &BTreeMap<ShardId, Bandwidth>,
    incoming_limits: &BTreeMap<ShardId, Bandwidth>,
    mut is_link_allowed: impl FnMut(ShardId, ShardId) -> bool,
) -> Bandwidth {
    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
    enum ShardNode {
        Sender(ShardId),
        Receiver(ShardId),
    }

    let mut shard_node_to_node_idx = BTreeMap::new();
    for shard_id in outgoing_limits.keys() {
        let next_idx = shard_node_to_node_idx.len();
        shard_node_to_node_idx.entry(ShardNode::Sender(*shard_id)).or_insert(next_idx);
    }

    for shard_id in incoming_limits.keys() {
        let next_idx = shard_node_to_node_idx.len();
        shard_node_to_node_idx.entry(ShardNode::Receiver(*shard_id)).or_insert(next_idx);
    }

    let source = shard_node_to_node_idx.len();
    let sink = shard_node_to_node_idx.len() + 1;

    let mut graph =
        max_flow_solver::NetworkFlowAdjacencyList::with_size(shard_node_to_node_idx.len() + 2)
            .and_source_sink(source, sink);

    fn toi64(val: &u64) -> i64 {
        (*val).try_into().expect("Can't convert u64 to i64")
    }

    for (sender_id, outgoing_limit) in outgoing_limits {
        let sender_node_idx = shard_node_to_node_idx.get(&ShardNode::Sender(*sender_id)).unwrap();
        graph.add_edge(source, *sender_node_idx, toi64(outgoing_limit));
    }

    for (receiver_id, incoming_limit) in incoming_limits {
        let receiver_node_idx =
            shard_node_to_node_idx.get(&ShardNode::Receiver(*receiver_id)).unwrap();
        graph.add_edge(*receiver_node_idx, sink, toi64(incoming_limit));
    }

    for sender_id in outgoing_limits.keys() {
        let sender_node_idx = shard_node_to_node_idx.get(&ShardNode::Sender(*sender_id)).unwrap();
        for receiver_id in incoming_limits.keys() {
            if !is_link_allowed(*sender_id, *receiver_id) {
                continue;
            }

            let receiver_node_idx =
                shard_node_to_node_idx.get(&ShardNode::Receiver(*receiver_id)).unwrap();

            graph.add_edge(*sender_node_idx, *receiver_node_idx, max_flow_solver::INF);
        }
    }

    let max_flow_i64 = max_flow_solver::DinicSolver::init(&mut graph).solve();
    max_flow_i64.try_into().expect("Can't convert i64 to u64")
}

/// Max flow algorithm taken from https://github.com/TianyiShi2001/Algorithms
/// I haven't verified its correctness, it's used only in tests anyway.
mod max_flow_solver {
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::rc::{Rc, Weak};

    /// This edge type is designed specifically for networkflow graphs.
    #[derive(Debug, Clone)]
    pub struct Edge {
        pub _from: usize,
        pub to: usize,
        pub flow: i64,
        pub capacity: i64,
        /// a weak reference to the residual edge that's pointing in the opposite direction
        pub residual: Weak<RefCell<Edge>>,
        pub _cost: i64,
        pub _original_cost: i64,
    }

    impl Edge {
        pub fn new_with_cost(
            from: usize,
            to: usize,
            capacity: i64,
            cost: i64,
        ) -> [Rc<RefCell<Self>>; 2] {
            let e1 = Rc::new(RefCell::new(Edge {
                _from: from,
                to,
                capacity,
                flow: 0,
                residual: Weak::default(),
                _cost: cost,
                _original_cost: cost,
            }));
            let e2 = Rc::new(RefCell::new(Edge {
                _from: to,
                to: from,
                capacity: 0,
                flow: 0,
                residual: Weak::default(),
                _cost: -cost,
                _original_cost: -cost,
            }));
            e1.borrow_mut().residual = Rc::downgrade(&e2);
            e2.borrow_mut().residual = Rc::downgrade(&e1);
            [e1, e2]
        }
        pub fn reamaining_capacity(&self) -> i64 {
            self.capacity - self.flow
        }
        pub fn augment(&mut self, bottleneck: i64) {
            self.flow += bottleneck;
            self.residual.upgrade().unwrap().borrow_mut().flow -= bottleneck;
        }
    }

    /// A type of adjacency list specifically used for network flow analysis
    #[derive(Debug)]
    pub struct NetworkFlowAdjacencyList {
        edges: Vec<Vec<Rc<RefCell<Edge>>>>,
        pub source: usize,
        pub sink: usize,
    }

    impl NetworkFlowAdjacencyList {
        /// Initialize an empty adjacency list that can hold up to n nodes.
        pub fn with_size(n: usize) -> Self {
            Self { edges: vec![vec![]; n], source: n - 1, sink: n - 2 }
        }
        pub fn and_source_sink(mut self, source: usize, sink: usize) -> Self {
            self.source = source;
            self.sink = sink;
            self
        }
        pub fn add_edge(&mut self, from: usize, to: usize, capacity: i64) {
            self.add_edge_with_cost(from, to, capacity, 0);
        }
        pub fn add_edge_with_cost(&mut self, from: usize, to: usize, capacity: i64, cost: i64) {
            let [e1, e2] = Edge::new_with_cost(from, to, capacity, cost);
            self.edges[from].push(e1);
            self.edges[to].push(e2);
        }
        pub fn node_count(&self) -> usize {
            self.edges.len()
        }
    }

    impl std::ops::Index<usize> for NetworkFlowAdjacencyList {
        type Output = Vec<Rc<RefCell<Edge>>>;
        fn index(&self, index: usize) -> &Self::Output {
            &self.edges[index]
        }
    }

    impl std::ops::IndexMut<usize> for NetworkFlowAdjacencyList {
        fn index_mut(&mut self, index: usize) -> &mut Self::Output {
            &mut self.edges[index]
        }
    }

    pub struct DinicSolver<'a> {
        g: &'a mut NetworkFlowAdjacencyList,
        n: usize,
        levels: Vec<isize>,
    }

    pub const INF: i64 = i64::MAX / 2;

    impl<'a> DinicSolver<'a> {
        pub fn init(g: &'a mut NetworkFlowAdjacencyList) -> Self {
            let n = g.node_count();
            Self { g, n, levels: vec![0; n] }
        }
        pub fn solve(&mut self) -> i64 {
            let mut max_flow: i64 = 0;

            while self.bfs() {
                // `next[i]` indicates the next unused edge index in the adjacency list for node `i`. This is part
                // of the Shimon Even and Alon Itai optimization of pruning deads ends as part of the DFS phase.
                let mut next = vec![0usize; self.n];
                // Find max flow by adding all augmenting path flows.
                let mut f = -1;
                while f != 0 {
                    f = self.dfs(self.g.source, &mut next, INF);
                    dbg!(f);
                    max_flow += f;
                }
            }
            max_flow
        }

        // for i in 0..self.n if (self.levels[i] != -1) minCut[i] = true;
        // }

        // Do a BFS from source to sink and compute the depth/level of each node
        // which is the minimum number of edges from that node to the source.
        fn bfs(&mut self) -> bool {
            self.levels = vec![-1; self.n];
            self.levels[self.g.source] = 0;
            let mut q = VecDeque::with_capacity(self.n);
            q.push_back(self.g.source);
            while let Some(node) = q.pop_front() {
                for edge in &self.g[node] {
                    let edge = edge.borrow();
                    let rcap = edge.reamaining_capacity();
                    if rcap > 0 && self.levels[edge.to] == -1 {
                        self.levels[edge.to] = self.levels[node] + 1;
                        q.push_back(edge.to)
                    }
                }
            }
            self.levels[self.g.sink] != -1
        }

        fn dfs(&mut self, at: usize, next: &mut [usize], flow: i64) -> i64 {
            if at == self.g.sink {
                return flow;
            }
            let num_edges = self.g[at].len();
            while next[at] < num_edges {
                let edge = unsafe { &*(&self.g[at][next[at]] as *const Rc<RefCell<Edge>>) };
                let mut _edge = edge.borrow_mut();
                let rcap = _edge.reamaining_capacity();
                if rcap > 0 && self.levels[_edge.to] == self.levels[at] + 1 {
                    let bottleneck = self.dfs(_edge.to, next, std::cmp::min(flow, rcap));
                    if bottleneck > 0 {
                        _edge.augment(bottleneck);
                        return bottleneck;
                    }
                }
                next[at] += 1;
            }

            0
        }
    }
}
