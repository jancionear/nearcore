use bitvec::order::Msb0;
use near_primitives::bandwidth_scheduler::{
    Bandwidth, BandwidthRequest, BANDWIDTH_REQUEST_BITMAP_SIZE,
    COMPRESSED_BANDWIDTH_REQUEST_VALUES_NUM,
};
use near_primitives::types::ShardId;

use super::BandwidthSchedulerParams;
use bitvec::array::BitArray;

struct RequestBitmap {
    map: BitArray<[u8; BANDWIDTH_REQUEST_BITMAP_SIZE], Msb0>,
}

impl RequestBitmap {
    pub fn new() -> RequestBitmap {
        Self::from_bytes([0u8; BANDWIDTH_REQUEST_BITMAP_SIZE])
    }

    pub fn from_bytes(bytes: [u8; BANDWIDTH_REQUEST_BITMAP_SIZE]) -> RequestBitmap {
        RequestBitmap { map: BitArray::new(bytes) }
    }

    pub fn to_bytes(&self) -> [u8; BANDWIDTH_REQUEST_BITMAP_SIZE] {
        self.map.data
    }

    pub fn get_bit(&self, index: usize) -> bool {
        Self::validate_index(index);
        self.map[index]
    }

    pub fn set_bit(&mut self, index: usize, value: bool) {
        Self::validate_index(index);
        self.map.set(index, value)
    }

    pub fn len(&self) -> usize {
        COMPRESSED_BANDWIDTH_REQUEST_VALUES_NUM
    }

    pub fn is_all_false(&self) -> bool {
        self.to_bytes() == [0u8; BANDWIDTH_REQUEST_BITMAP_SIZE]
    }

    fn validate_index(index: usize) {
        if index >= COMPRESSED_BANDWIDTH_REQUEST_VALUES_NUM {
            panic!(
                "RequestBitmap - index too large! {} >= {}",
                index, COMPRESSED_BANDWIDTH_REQUEST_VALUES_NUM
            );
        }
    }
}

pub struct UncompressedBandwidthRequest {
    pub to_shard: ShardId,
    pub requested_values: Vec<Bandwidth>,
}

impl UncompressedBandwidthRequest {
    pub fn from_compressed(
        compressed: &BandwidthRequest,
        params: &BandwidthSchedulerParams,
    ) -> UncompressedBandwidthRequest {
        let values = BandwidthRequestValues::new(params);
        let bitmap = RequestBitmap::from_bytes(compressed.requested_values_bitmap_bytes);

        let mut requested_values = Vec::new();
        for (i, value) in values.0.iter().enumerate() {
            if bitmap.get_bit(i) {
                requested_values.push(*value);
            }
        }

        UncompressedBandwidthRequest { to_shard: compressed.to_shard.into(), requested_values }
    }
}

/// Values of bandwidth that can be requested in a bandwidth request.
/// When the nth bit is set in a request bitmap, it means that a shard is requesting the nth value from this list.
pub struct BandwidthRequestValues(pub [Bandwidth; COMPRESSED_BANDWIDTH_REQUEST_VALUES_NUM]);

impl BandwidthRequestValues {
    pub fn new(params: &BandwidthSchedulerParams) -> BandwidthRequestValues {
        // values[-1] = base_bandwidth
        // values[values.len() - 1] = max_bandwidth
        // values[i] = linear interpolation between values[-1] and values[values.len() - 1]
        let mut values = [0; COMPRESSED_BANDWIDTH_REQUEST_VALUES_NUM];
        let values_len: u64 =
            values.len().try_into().expect("Converting usize to u64 shouldn't fail");
        for i in 0..values_len {
            values[i as usize] = params.base_bandwidth
                + (params.max_shard_bandwidth - params.base_bandwidth) * (i + 1) / values_len;
        }

        // The value that is closest to MAX_RECEIPT_SIZE is set to MAX_RECEIPT_SIZE.
        // This ensures that the value corresponding to max size receipts can be granted after base bandwidth is granted.
        let mut closest_to_max: u64 = 0;
        for value in &values {
            if value.abs_diff(params.max_receipt_size)
                < closest_to_max.abs_diff(params.max_receipt_size)
            {
                closest_to_max = *value;
            }
        }
        for value in values.iter_mut() {
            if *value == closest_to_max {
                *value = params.max_receipt_size;
            }
        }

        BandwidthRequestValues(values)
    }
}

pub fn make_bandwidth_request_from_receipt_sizes(
    to_shard: ShardId,
    receipt_sizes: impl Iterator<Item = u64>,
    params: &BandwidthSchedulerParams,
) -> Option<BandwidthRequest> {
    let values = BandwidthRequestValues::new(params);
    let mut bitmap = RequestBitmap::new();

    let mut total_size: u64 = 0;
    let mut cur_value_idx: usize = 0;
    for receipt_size in receipt_sizes {
        total_size = total_size
            .checked_add(receipt_size)
            .expect("Total size of receipts doesn't fit in u64, are there exabytes of receipts?");

        if total_size <= params.base_bandwidth {
            continue;
        }

        // Find a value that is at least as big as the total_size
        while cur_value_idx < values.0.len() && values.0[cur_value_idx] < total_size {
            cur_value_idx += 1;
        }

        if cur_value_idx == values.0.len() {
            bitmap.set_bit(bitmap.len() - 1, true);
            break;
        }

        // Request the value thath is at least as large as total_size
        bitmap.set_bit(cur_value_idx, true);
    }

    if bitmap.is_all_false() {
        return None;
    }

    let to_shard_u8: u8 = to_shard.try_into().expect("Shard id above 255!");

    Some(BandwidthRequest {
        to_shard: to_shard_u8,
        requested_values_bitmap_bytes: bitmap.to_bytes(),
    })
}
