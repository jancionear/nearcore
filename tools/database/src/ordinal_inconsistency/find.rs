use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use near_chain::{ChainStore, Error};
use near_primitives::hash::CryptoHash;

use crate::ordinal_inconsistency::timer::WorkTimer;

use super::OrdinalInconsistency;
use super::read_db::{HashIndex, ReadDbData};

pub fn find_ordinal_inconsistencies(
    chain_store: &ChainStore,
) -> Result<Vec<OrdinalInconsistency>, Error> {
    let db_data = Arc::new(super::read_db::read_db_data(chain_store)?);

    let num_threads = 128;
    let (update_sender, update_receiver) = std::sync::mpsc::channel::<FindInconsistenciesUpdate>();
    let mut threads = Vec::with_capacity(num_threads);
    for thread_id in 0..num_threads {
        let db_data = db_data.clone();
        let update_sender = update_sender.clone();
        threads.push(std::thread::spawn(move || {
            find_inconsistencies_thread(&db_data, &update_sender, thread_id, num_threads)
        }));
    }
    std::mem::drop(update_sender);

    let mut found_inconsistencies = Vec::new();
    let mut processed_counter = 0;
    let mut timer = WorkTimer::new("Scan for inconsistencies", db_data.height_to_block_hash.len());

    while let Ok(update) = update_receiver.recv() {
        match update {
            FindInconsistenciesUpdate::Inconsistency(inconsistency) => {
                found_inconsistencies.push(inconsistency);
            }
            FindInconsistenciesUpdate::Processed(count) => {
                processed_counter += count;
                timer.update_total(processed_counter);
            }
        }
    }
    timer.finish();

    for thread in threads {
        thread.join().unwrap();
    }

    let db_data: ReadDbData =
        Arc::<ReadDbData>::try_unwrap(db_data).expect(" Should have exactly one owner");
    let ReadDbData {
        height_to_block_hash,
        block_hash_to_ordinal,
        ordinal_to_block_hash,
        hash_to_index,
    } = db_data;
    std::mem::drop(height_to_block_hash);
    std::mem::drop(block_hash_to_ordinal);
    std::mem::drop(ordinal_to_block_hash);

    // Convert HashIndex to CryptoHash
    let mut timer = WorkTimer::new("Convert HashIndex to CryptoHash", found_inconsistencies.len());
    let mut need_hash_for_index: HashSet<HashIndex> =
        HashSet::with_capacity(found_inconsistencies.len() * 2);
    for inconsistency in &found_inconsistencies {
        need_hash_for_index.insert(inconsistency.correct_block_hash);
        need_hash_for_index.insert(inconsistency.actual_block_hash);
    }

    let mut index_to_hash: HashMap<HashIndex, CryptoHash> =
        HashMap::with_capacity(need_hash_for_index.len());
    for (i, (hash, index)) in hash_to_index.iter().enumerate() {
        if need_hash_for_index.contains(&index) {
            index_to_hash.insert(*index, *hash);
        }
        timer.update_total(i);
    }

    let mut result = Vec::with_capacity(found_inconsistencies.len());
    for inconsistency in found_inconsistencies {
        let correct_block_hash = index_to_hash
            .get(&inconsistency.correct_block_hash)
            .cloned()
            .unwrap_or_else(|| CryptoHash::default());
        let actual_block_hash = index_to_hash
            .get(&inconsistency.actual_block_hash)
            .cloned()
            .unwrap_or_else(|| CryptoHash::default());

        result.push(OrdinalInconsistency {
            block_height: inconsistency.block_height.into(),
            block_ordinal: inconsistency.block_ordinal.into(),
            correct_block_hash,
            actual_block_hash,
        });
    }
    result.sort_by_key(|i| i.block_height);

    println!("Found {} inconsistencies", result.len());
    for inconsistency in &result {
        println!(
            "Height: {}, Ordinal: {}, Correct Hash: {}, Actual Hash: {}",
            inconsistency.block_height,
            inconsistency.block_ordinal,
            inconsistency.correct_block_hash,
            inconsistency.actual_block_hash
        );
    }

    Ok(result)
}

enum FindInconsistenciesUpdate {
    Inconsistency(FoundInconsistency),
    Processed(usize),
}

struct FoundInconsistency {
    block_height: u32,
    block_ordinal: u32,
    correct_block_hash: HashIndex,
    actual_block_hash: HashIndex,
}

fn find_inconsistencies_thread(
    db_data: &super::read_db::ReadDbData,
    update_sender: &std::sync::mpsc::Sender<FindInconsistenciesUpdate>,
    thread_id: usize,
    num_threads: usize,
) {
    let ReadDbData { height_to_block_hash, block_hash_to_ordinal, ordinal_to_block_hash, .. } =
        &db_data;

    let mut processed_counter = 0;

    for i in (thread_id..height_to_block_hash.len()).step_by(num_threads) {
        let (height, block_hash) = height_to_block_hash[i];

        if let Some(block_ordinal) = block_hash_to_ordinal.get(&block_hash) {
            if let Some(hash_at_ordinal) = ordinal_to_block_hash.get(&block_ordinal) {
                if *hash_at_ordinal != block_hash {
                    update_sender
                        .send(FindInconsistenciesUpdate::Inconsistency(FoundInconsistency {
                            block_height: (height).into(),
                            block_ordinal: (*block_ordinal).into(),
                            correct_block_hash: block_hash,
                            actual_block_hash: *hash_at_ordinal,
                        }))
                        .unwrap();
                }
            }
        }

        processed_counter += 1;
        if processed_counter == 1000 {
            update_sender.send(FindInconsistenciesUpdate::Processed(processed_counter)).unwrap();
            processed_counter = 0;
        }
    }
    update_sender.send(FindInconsistenciesUpdate::Processed(processed_counter)).unwrap();
}
