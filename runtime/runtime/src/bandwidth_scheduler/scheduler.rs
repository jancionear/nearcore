use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use near_primitives::bandwidth_scheduler::{
    Bandwidth, BandwidthRequest, BandwidthRequests, BandwidthSchedulerState, ShardLink,
};
use near_primitives::types::ShardId;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;

use super::request::UncompressedBandwidthRequest;
use super::{distribute_remaining, BandwidthSchedulerOutput, BandwidthSchedulerParams};

pub struct ShardCongestionStatus {
    pub is_fully_congested: bool,
    pub allowed_sender_shard: ShardId,
    pub was_last_chunk_missed: bool,
}

pub struct BandwidthScheduler {
    shard_ids: Rc<[ShardId]>,
    shards_congestion_status: BTreeMap<ShardId, ShardCongestionStatus>,
    allowances: BTreeMap<ShardLink, Bandwidth>,
    granted_bandwidth: BTreeMap<ShardLink, Bandwidth>,
    incoming_limits: BTreeMap<ShardId, Bandwidth>,
    outgoing_limits: BTreeMap<ShardId, Bandwidth>,
    params: BandwidthSchedulerParams,
    rng: ChaCha20Rng,
}

impl BandwidthScheduler {
    pub fn new(
        mut shard_ids: Vec<ShardId>,
        shards_congestion_status: BTreeMap<ShardId, ShardCongestionStatus>,
        scheduler_state: BandwidthSchedulerState,
        rng_seed: [u8; 32],
        params: BandwidthSchedulerParams,
    ) -> BandwidthScheduler {
        let rng = ChaCha20Rng::from_seed(rng_seed);

        shard_ids.sort();
        let shard_ids_rc = Rc::from(shard_ids.as_slice());

        BandwidthScheduler {
            shard_ids: shard_ids_rc,
            shards_congestion_status,
            allowances: scheduler_state.allowances,
            granted_bandwidth: BTreeMap::new(),
            incoming_limits: BTreeMap::new(),
            outgoing_limits: BTreeMap::new(),
            params,
            rng,
        }
    }

    pub fn run(
        mut self,
        bandwidth_requests: &BTreeMap<ShardId, BandwidthRequests>,
    ) -> (BandwidthSchedulerOutput, BandwidthSchedulerState) {
        self.init_outgoing_and_incoming_limits();
        self.give_out_allowance();
        self.grant_base_bandwidth();
        self.process_bandwidth_requests(bandwidth_requests);
        self.distribute_remaining_bandwidth();

        let output = BandwidthSchedulerOutput {
            granted_bandwidth: self.granted_bandwidth,
            params: self.params,
        };
        let new_state = BandwidthSchedulerState { allowances: self.allowances };
        (output, new_state)
    }

    fn init_outgoing_and_incoming_limits(&mut self) {
        self.outgoing_limits =
            self.shard_ids.iter().map(|sid| (*sid, self.params.max_shard_bandwidth)).collect();
        self.incoming_limits =
            self.shard_ids.iter().map(|sid| (*sid, self.params.max_shard_bandwidth)).collect();

        for (receiver_id, congestion_status) in &self.shards_congestion_status {
            // Don't send anything to shards where the last chunk was missing
            if congestion_status.was_last_chunk_missed {
                self.incoming_limits.insert(*receiver_id, 0);
            }
        }
    }

    fn give_out_allowance(&mut self) {
        let num_shards: u64 = self.shard_ids.len().try_into().expect("Can't convert usize to u64");
        let allowance_per_height = self.params.max_shard_bandwidth / num_shards;
        for sender_id in self.shards_iter() {
            for receiver_id in self.shards_iter() {
                self.add_allowance(ShardLink::new(sender_id, receiver_id), allowance_per_height);
            }
        }
    }

    fn grant_base_bandwidth(&mut self) {
        for sender_id in self.shards_iter() {
            for receiver_id in self.shards_iter() {
                let link = ShardLink::new(sender_id, receiver_id);
                if self.is_link_allowed(&link) {
                    let _ = self.try_grant_additional_bandwidth(link, self.params.base_bandwidth);
                }
            }
        }
    }

    fn process_bandwidth_requests(&mut self, requests: &BTreeMap<ShardId, BandwidthRequests>) {
        let mut requests_by_allowance: BTreeMap<Bandwidth, RequestGroup> = BTreeMap::new();

        for (sender_shard, requests) in requests {
            let requests_list = match requests {
                BandwidthRequests::V1(requests_v1) => &requests_v1.requests,
            };

            for request in requests_list {
                let shard_link = ShardLink { from: *sender_shard, to: request.to_shard.into() };

                // Ignore requests on forbidden links, we can't grant anything there.
                if !self.is_link_allowed(&shard_link) {
                    continue;
                }

                let increases_request = BandwidthIncreaseRequests::from_bandwidth_request(
                    shard_link,
                    request,
                    &self.params,
                );
                let allowance = self.get_allowance(shard_link);
                requests_by_allowance
                    .entry(allowance)
                    .or_default()
                    .requests
                    .push(increases_request);
            }
        }

        while let Some((_allowance, mut request_group)) = requests_by_allowance.pop_last() {
            request_group.requests.shuffle(&mut self.rng);

            for mut request in request_group.requests {
                let Some(bandwidth_increase) = request.bandwidth_increases.pop_front() else {
                    continue;
                };

                if self
                    .try_grant_additional_bandwidth(request.shard_link, bandwidth_increase)
                    .is_ok()
                {
                    self.decrease_allowance(request.shard_link, bandwidth_increase);
                    let new_allowance = self.get_allowance(request.shard_link);
                    requests_by_allowance.entry(new_allowance).or_default().requests.push(request);
                }
            }
        }
    }

    fn distribute_remaining_bandwidth(&mut self) {
        let remaining_grants = distribute_remaining::distribute_remaining_bandwidth(
            &self.outgoing_limits,
            &self.incoming_limits,
            |a, b| self.is_link_allowed(&ShardLink::new(a, b)),
        );

        for (link, grant) in remaining_grants {
            self.try_grant_additional_bandwidth(link, grant)
                .expect("Granting remaing bandwidth must suceed");
        }
    }

    /// Iterate over shards without borrowing &self
    /// Allows to modify &mut self while iterating over all shard ids
    fn shards_iter(&self) -> impl Iterator<Item = ShardId> {
        // Clone the Rc that contains the shard ids
        let cloned_shard_ids = self.shard_ids.clone();

        // Create a closure which captures the cloned Rc and returns the nth shard id when called
        let get_shard_at_index = move |idx: usize| -> ShardId { cloned_shard_ids[idx] };

        // Run the closure for every index
        (0..self.shard_ids.len()).map(get_shard_at_index)
    }

    fn is_link_allowed(&self, link: &ShardLink) -> bool {
        let congestion_status = match self.shards_congestion_status.get(&link.to) {
            Some(status) => status,
            None => return true,
        };

        if congestion_status.was_last_chunk_missed {
            return false;
        }

        if congestion_status.is_fully_congested
            && link.from != congestion_status.allowed_sender_shard
        {
            return false;
        }

        true
    }

    fn try_grant_additional_bandwidth(
        &mut self,
        shard_link: ShardLink,
        bandwidth_increase: Bandwidth,
    ) -> Result<(), CantGrantBandwidthError> {
        let outgoing_limit = self.outgoing_limits.entry(shard_link.from).or_insert(0);
        let incoming_limit = self.incoming_limits.entry(shard_link.to).or_insert(0);

        if bandwidth_increase > *outgoing_limit || bandwidth_increase > *incoming_limit {
            return Err(CantGrantBandwidthError);
        }

        *self.granted_bandwidth.entry(shard_link).or_insert(0) += bandwidth_increase;
        *outgoing_limit -= bandwidth_increase;
        *incoming_limit -= bandwidth_increase;

        Ok(())
    }

    fn get_allowance(&mut self, shard_link: ShardLink) -> Bandwidth {
        self.allowances.get(&shard_link).copied().unwrap_or_default()
    }

    fn set_allowance(&mut self, shard_link: ShardLink, amount: Bandwidth) {
        self.allowances.insert(shard_link, amount);
    }

    fn add_allowance(&mut self, shard_link: ShardLink, amount: Bandwidth) {
        let mut cur_allowance = self.get_allowance(shard_link);
        cur_allowance += amount;
        if cur_allowance > self.params.max_allowance {
            cur_allowance = self.params.max_allowance;
        }

        self.set_allowance(shard_link, cur_allowance);
    }

    fn decrease_allowance(&mut self, shard_link: ShardLink, amount: Bandwidth) {
        let cur_allowance = self.get_allowance(shard_link);
        let new_allowance = cur_allowance.saturating_sub(amount);
        self.set_allowance(shard_link, new_allowance);
    }
}

#[derive(Clone, Copy, Debug)]
struct CantGrantBandwidthError;

/// A group of bandwidth requests with the same link allowance
#[derive(Debug, Default)]
struct RequestGroup {
    requests: Vec<BandwidthIncreaseRequests>,
}

/// A BandwidthRequest translated to a format where each "option" is an increase over the previous option instead of an absolute granted value.
#[derive(Debug)]
struct BandwidthIncreaseRequests {
    /// The shard link on which the bandwidth is requested.
    shard_link: ShardLink,
    /// Each of the entries in the queue describes how much additional bandwidth should be granted.
    bandwidth_increases: VecDeque<Bandwidth>,
}

impl BandwidthIncreaseRequests {
    fn from_bandwidth_request(
        shard_link: ShardLink,
        bandwidth_request: &BandwidthRequest,
        params: &BandwidthSchedulerParams,
    ) -> BandwidthIncreaseRequests {
        // Get the absolute values of requested bandwidth from bandwidth request.
        let uncompressed = UncompressedBandwidthRequest::from_compressed(bandwidth_request, params);
        assert_eq!(uncompressed.to_shard, shard_link.to);

        let mut bandwidth_increases = VecDeque::new();
        let mut last_option = params.base_bandwidth;
        for bandwidth_option in uncompressed.requested_values {
            let increase = bandwidth_option.saturating_sub(last_option);
            if increase > 0 {
                bandwidth_increases.push_back(increase);
                last_option = bandwidth_option;
            }
        }

        BandwidthIncreaseRequests { shard_link, bandwidth_increases }
    }
}
