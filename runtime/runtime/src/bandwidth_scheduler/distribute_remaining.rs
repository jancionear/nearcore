use std::collections::BTreeMap;

use near_primitives::bandwidth_scheduler::{Bandwidth, ShardLink};
use near_primitives::types::ShardId;

/// Magic algorithm which distributes the remaining bandwidth in a fair way (∩ ͡° ͜ʖ ͡°)⊃━☆ﾟ. * ･ ｡ﾟ,
/// The arguments describe how much spare bandwidth there is on the left (sending) shards and right (receiving) shards.
/// The function grants some additional bandwidth on all the links to make use of the leftover bandwidth.

pub fn distribute_remaining_bandwidth(
    left: &BTreeMap<ShardId, Bandwidth>,
    right: &BTreeMap<ShardId, Bandwidth>,
    mut is_link_allowed: impl FnMut(ShardId, ShardId) -> bool,
) -> BTreeMap<ShardLink, Bandwidth> {
    fn init_info_fn((shard_id, bandwidth_left): (&ShardId, &Bandwidth)) -> (ShardId, Info) {
        (*shard_id, Info { bandwidth_left: *bandwidth_left, links_num: 0 })
    }
    let mut left_infos: BTreeMap<ShardId, Info> = left.iter().map(init_info_fn).collect();
    let mut right_infos: BTreeMap<ShardId, Info> = right.iter().map(init_info_fn).collect();

    for left_id in left.keys() {
        let Some(left_info) = left_infos.get_mut(left_id) else {
            continue;
        };
        for right_id in right.keys() {
            let Some(right_info) = right_infos.get_mut(left_id) else {
                continue;
            };
            if is_link_allowed(*left_id, *right_id) {
                left_info.links_num += 1;
                right_info.links_num += 1;
            }
        }
    }

    let mut left_by_avg_link_bandwidth: Vec<(Bandwidth, ShardId)> = left
        .keys()
        .map(|shard| {
            let avg_link_bandwidth =
                left_infos.get(shard).map(|info| info.average_link_bandwidth()).unwrap_or(0);
            (avg_link_bandwidth, *shard)
        })
        .collect();
    left_by_avg_link_bandwidth.sort();

    let mut right_by_avg_link_bandwidth: Vec<(Bandwidth, ShardId)> = left
        .keys()
        .map(|shard| {
            let avg_link_bandwidth =
                right_infos.get(shard).map(|info| info.average_link_bandwidth()).unwrap_or(0);
            (avg_link_bandwidth, *shard)
        })
        .collect();
    right_by_avg_link_bandwidth.sort();

    let mut bandwidth_grants: BTreeMap<ShardLink, Bandwidth> = BTreeMap::new();
    for left_id in left_by_avg_link_bandwidth.iter().map(|(_, shard)| *shard) {
        let Some(left_info) = left_infos.get_mut(&left_id) else {
            continue;
        };
        for right_id in right_by_avg_link_bandwidth.iter().map(|(_, shard)| *shard) {
            if !is_link_allowed(left_id, right_id) {
                continue;
            }

            let Some(right_info) = right_infos.get_mut(&right_id) else {
                continue;
            };

            if left_info.links_num == 0 || right_info.links_num == 0 {
                break;
            }

            let left_proposition = left_info.link_proposition();
            let right_proposition = right_info.link_proposition();
            let granted_bandwidth = std::cmp::min(left_proposition, right_proposition);
            bandwidth_grants.insert(ShardLink::new(left_id, right_id), granted_bandwidth);

            left_info.bandwidth_left -= granted_bandwidth;
            left_info.links_num -= 1;

            right_info.bandwidth_left -= granted_bandwidth;
            right_info.links_num -= 1;
        }
    }

    bandwidth_grants
}

struct Info {
    bandwidth_left: Bandwidth,
    links_num: u64,
}

impl Info {
    fn average_link_bandwidth(&self) -> Bandwidth {
        if self.links_num == 0 {
            return 0;
        }
        self.bandwidth_left / self.links_num
    }

    fn link_proposition(&self) -> Bandwidth {
        self.bandwidth_left / self.links_num + self.bandwidth_left % self.links_num
    }
}
