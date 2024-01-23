use itertools::Itertools;
use near_async::messaging::{CanSend, Sender};
use near_chain::chain::{
    apply_new_chunk, apply_old_chunk, NewChunkData, NewChunkResult, OldChunkData, OldChunkResult,
    ShardContext, StorageContext,
};
use near_chain::types::{
    ApplyChunkBlockContext, ApplyChunkResult, RuntimeAdapter, StorageDataSource,
};
use near_chain::validate::validate_chunk_with_chunk_extra_and_receipts_root;
use near_chain::{Block, BlockHeader, Chain, ChainStore, ChainStoreAccess};
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::{NetworkRequests, PeerManagerMessageRequest};
use near_primitives::checked_feature;
use near_primitives::chunk_validation::{
    ChunkEndorsement, ChunkStateTransition, ChunkStateWitness, StoredChunkStateTransitionData,
};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::merkle::merklize;
use near_primitives::sharding::{ChunkHash, ShardChunk, ShardChunkHeader};
use near_primitives::types::chunk_extra::ChunkExtra;
use near_primitives::types::{AccountId, EpochId};
use near_primitives::validator_signer::ValidatorSigner;
use near_primitives::{challenge::PartialState, sharding::ReceiptProof};
use near_store::PartialStorage;
use std::sync::Arc;
use std::{borrow::Cow, collections::HashMap};

use crate::Client;

// This is the number of unique chunks for which we would track the chunk endorsements.
// Ideally, we should not be processing more than num_shards chunks at a time.
const NUM_CHUNK_ENDORSEMENTS_CACHE_COUNT: usize = 100;

// After validating a chunk state witness, we ideally need to send the chunk endorsement
// to just the next block producer at height h. However, it's possible that blocks at height
// h may be skipped and block producer at height h+1 picks up the chunk. We need to ensure
// that these later block producers also receive the chunk endorsement.
// Keeping a threshold of 5 block producers should be sufficient for most scenarios.
const NUM_NEXT_BLOCK_PRODUCERS_TO_SEND_CHUNK_ENDORSEMENT: u64 = 5;

/// A module that handles chunk validation logic. Chunk validation refers to a
/// critical process of stateless validation, where chunk validators (certain
/// validators selected to validate the chunk) verify that the chunk's state
/// witness is correct, and then send chunk endorsements to the block producer
/// so that the chunk can be included in the block.
pub struct ChunkValidator {
    /// The signer for our own node, if we are a validator. If not, this is None.
    my_signer: Option<Arc<dyn ValidatorSigner>>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    network_sender: Sender<PeerManagerMessageRequest>,
    runtime_adapter: Arc<dyn RuntimeAdapter>,

    /// We store the validated chunk endorsements received from chunk validators
    /// This is keyed on chunk_hash and account_id of validator to avoid duplicates.
    /// Chunk endorsements would later be used as a part of block production.
    chunk_endorsements: lru::LruCache<ChunkHash, HashMap<AccountId, ChunkEndorsement>>,
}

impl ChunkValidator {
    pub fn new(
        my_signer: Option<Arc<dyn ValidatorSigner>>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        network_sender: Sender<PeerManagerMessageRequest>,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
    ) -> Self {
        Self {
            my_signer,
            epoch_manager,
            network_sender,
            runtime_adapter,
            chunk_endorsements: lru::LruCache::new(NUM_CHUNK_ENDORSEMENTS_CACHE_COUNT),
        }
    }

    /// Performs the chunk validation logic. When done, it will send the chunk
    /// endorsement message to the block producer. The actual validation logic
    /// happens in a separate thread.
    pub fn start_validating_chunk(
        &self,
        state_witness: ChunkStateWitness,
        chain_store: &ChainStore,
    ) -> Result<(), Error> {
        let chunk_header = state_witness.chunk_header.clone();
        let Some(my_signer) = self.my_signer.as_ref() else {
            return Err(Error::NotAValidator);
        };
        let epoch_id =
            self.epoch_manager.get_epoch_id_from_prev_block(chunk_header.prev_block_hash())?;
        // We will only validate something if we are a chunk validator for this chunk.
        // Note this also covers the case before the protocol upgrade for chunk validators,
        // because the chunk validators will be empty.
        let chunk_validator_assignments = self.epoch_manager.get_chunk_validator_assignments(
            &epoch_id,
            chunk_header.shard_id(),
            chunk_header.height_created(),
        )?;
        if !chunk_validator_assignments.contains(my_signer.validator_id()) {
            return Err(Error::NotAChunkValidator);
        }

        let pre_validation_result = pre_validate_chunk_state_witness(
            &state_witness,
            chain_store,
            self.epoch_manager.as_ref(),
        )?;

        // Send the chunk endorsement to the next NUM_NEXT_BLOCK_PRODUCERS_TO_SEND_CHUNK_ENDORSEMENT block producers.
        // It's possible we may reach the end of the epoch, in which case, ignore the error from get_block_producer.
        let block_height = chunk_header.height_created();
        let block_producers = (0..NUM_NEXT_BLOCK_PRODUCERS_TO_SEND_CHUNK_ENDORSEMENT)
            .map_while(|i| self.epoch_manager.get_block_producer(&epoch_id, block_height + i).ok())
            .collect_vec();
        assert!(!block_producers.is_empty());

        let network_sender = self.network_sender.clone();
        let signer = self.my_signer.clone().unwrap();
        let epoch_manager = self.epoch_manager.clone();
        let runtime_adapter = self.runtime_adapter.clone();
        rayon::spawn(move || {
            match validate_chunk_state_witness(
                state_witness,
                pre_validation_result,
                epoch_manager.as_ref(),
                runtime_adapter.as_ref(),
            ) {
                Ok(()) => {
                    tracing::debug!(
                        target: "chunk_validation",
                        chunk_hash=?chunk_header.chunk_hash(),
                        ?block_producers,
                        "Chunk validated successfully, sending endorsement",
                    );
                    let endorsement = ChunkEndorsement::new(chunk_header.chunk_hash(), signer);
                    for block_producer in block_producers {
                        network_sender.send(PeerManagerMessageRequest::NetworkRequests(
                            NetworkRequests::ChunkEndorsement(block_producer, endorsement.clone()),
                        ));
                    }
                }
                Err(err) => {
                    tracing::error!("Failed to validate chunk: {:?}", err);
                }
            }
        });
        Ok(())
    }
}

/// Pre-validates the chunk's receipts and transactions against the chain.
/// We do this before handing off the computationally intensive part to a
/// validation thread.
fn pre_validate_chunk_state_witness(
    state_witness: &ChunkStateWitness,
    store: &ChainStore,
    epoch_manager: &dyn EpochManagerAdapter,
) -> Result<PreValidationOutput, Error> {
    let shard_id = state_witness.chunk_header.shard_id();

    // First, go back through the blockchain history to locate the last new chunk
    // and last last new chunk for the shard.

    // Blocks from the last new chunk (exclusive) to the parent block (inclusive).
    let mut blocks_after_last_chunk = Vec::new();
    // Blocks from the last last new chunk (exclusive) to the last new chunk (inclusive).
    let mut blocks_after_last_last_chunk = Vec::new();

    {
        let mut block_hash = *state_witness.chunk_header.prev_block_hash();
        let mut prev_chunks_seen = 0;
        loop {
            let block = store.get_block(&block_hash)?;
            let chunks = block.chunks();
            let Some(chunk) = chunks.get(shard_id as usize) else {
                return Err(Error::InvalidChunkStateWitness(format!(
                    "Shard {} does not exist in block {:?}",
                    shard_id, block_hash
                )));
            };
            let is_new_chunk = chunk.is_new_chunk(block.header().height());
            block_hash = *block.header().prev_hash();
            if prev_chunks_seen == 0 {
                blocks_after_last_chunk.push(block);
            } else if prev_chunks_seen == 1 {
                blocks_after_last_last_chunk.push(block);
            }
            if is_new_chunk {
                prev_chunks_seen += 1;
            }
            if prev_chunks_seen == 2 {
                break;
            }
        }
    }

    // Compute the chunks from which receipts should be collected.
    // let mut chunks_to_collect_receipts_from = Vec::new();
    // for block in blocks_after_last_last_chunk.iter().rev() {
    //     // To stay consistent with the order in which receipts are applied,
    //     // blocks are iterated in reverse order (from new to old), and
    //     // chunks are shuffled for each block.
    //     let mut chunks_in_block = block
    //         .chunks()
    //         .iter()
    //         .map(|chunk| (chunk.chunk_hash(), chunk.prev_outgoing_receipts_root()))
    //         .collect::<Vec<_>>();
    //     shuffle_receipt_proofs(&mut chunks_in_block, block.hash());
    //     chunks_to_collect_receipts_from.extend(chunks_in_block);
    // }

    // Verify that for each chunk, the receipts that have been provided match
    // the receipts that we are expecting.
    // let mut receipts_to_apply = Vec::new();
    // for (chunk_hash, receipt_root) in chunks_to_collect_receipts_from {
    //     let Some(receipt_proof) = state_witness.source_receipt_proofs.get(&chunk_hash) else {
    //         return Err(Error::InvalidChunkStateWitness(format!(
    //             "Missing source receipt proof for chunk {:?}",
    //             chunk_hash
    //         )));
    //     };
    //     if !receipt_proof.verify_against_receipt_root(receipt_root) {
    //         return Err(Error::InvalidChunkStateWitness(format!(
    //             "Provided receipt proof failed verification against receipt root for chunk {:?}",
    //             chunk_hash
    //         )));
    //     }
    //     // TODO(#10265): This does not currently handle shard layout change.
    //     if receipt_proof.1.to_shard_id != shard_id {
    //         return Err(Error::InvalidChunkStateWitness(format!(
    //             "Receipt proof for chunk {:?} is for shard {}, expected shard {}",
    //             chunk_hash, receipt_proof.1.to_shard_id, shard_id
    //         )));
    //     }
    //     receipts_to_apply.extend(receipt_proof.0.iter().cloned());
    // }
    let (last_chunk_block, implicit_transition_blocks) =
        blocks_after_last_chunk.split_last().unwrap();
    let receipts_response = &store.get_incoming_receipts_for_shard(
        epoch_manager,
        shard_id,
        *last_chunk_block.header().hash(),
        blocks_after_last_last_chunk.last().unwrap().header().height(),
    )?;
    let receipts_to_apply = near_chain::chain::collect_receipts_from_response(receipts_response);
    let applied_receipts_hash = hash(&borsh::to_vec(receipts_to_apply.as_slice()).unwrap());
    if applied_receipts_hash != state_witness.applied_receipts_hash {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Receipts hash {:?} does not match expected receipts hash {:?}",
            applied_receipts_hash, state_witness.applied_receipts_hash
        )));
    }
    let (tx_root_from_state_witness, _) = merklize(&state_witness.transactions);
    let last_new_chunk_tx_root =
        last_chunk_block.chunks().get(shard_id as usize).unwrap().tx_root();
    if last_new_chunk_tx_root != tx_root_from_state_witness {
        return Err(Error::InvalidChunkStateWitness(format!(
            "Transaction root {:?} does not match expected transaction root {:?}",
            tx_root_from_state_witness, last_new_chunk_tx_root
        )));
    }

    Ok(PreValidationOutput {
        main_transition_params: NewChunkData {
            chunk_header: last_chunk_block.chunks().get(shard_id as usize).unwrap().clone(),
            transactions: state_witness.transactions.clone(),
            receipts: receipts_to_apply,
            resharding_state_roots: None,
            block: Chain::get_apply_chunk_block_context(
                epoch_manager,
                last_chunk_block.header(),
                &store.get_previous_header(last_chunk_block.header())?,
                true,
            )?,
            is_first_block_with_chunk_of_version: false,
            storage_context: StorageContext {
                storage_data_source: StorageDataSource::Recorded(PartialStorage {
                    nodes: state_witness.main_state_transition.base_state.clone(),
                }),
                state_patch: Default::default(),
                record_storage: false,
            },
        },
        implicit_transition_params: implicit_transition_blocks
            .into_iter()
            .rev()
            .map(|block| -> Result<_, Error> {
                Ok(Chain::get_apply_chunk_block_context(
                    epoch_manager,
                    block.header(),
                    &store.get_previous_header(block.header())?,
                    false,
                )?)
            })
            .collect::<Result<_, _>>()?,
    })
}

#[allow(unused)]
struct PreValidationOutput {
    main_transition_params: NewChunkData,
    implicit_transition_params: Vec<ApplyChunkBlockContext>,
}

fn validate_chunk_state_witness(
    state_witness: ChunkStateWitness,
    pre_validation_output: PreValidationOutput,
    epoch_manager: &dyn EpochManagerAdapter,
    runtime_adapter: &dyn RuntimeAdapter,
) -> Result<(), Error> {
    let span = tracing::debug_span!(target: "chain", "validate_chunk_state_witness").entered();
    let main_transition = pre_validation_output.main_transition_params;
    let chunk_header = main_transition.chunk_header.clone();
    let epoch_id = epoch_manager.get_epoch_id(&main_transition.block.block_hash)?;
    let shard_uid =
        epoch_manager.shard_id_to_uid(main_transition.chunk_header.shard_id(), &epoch_id)?;
    // Should we validate other fields?
    let NewChunkResult { apply_result: mut main_apply_result, .. } = apply_new_chunk(
        &span,
        main_transition,
        ShardContext {
            shard_uid,
            cares_about_shard_this_epoch: true,
            will_shard_layout_change: false,
            should_apply_chunk: true,
            need_to_reshard: false,
        },
        runtime_adapter,
        epoch_manager,
    )?;
    let outgoing_receipts = std::mem::take(&mut main_apply_result.outgoing_receipts);
    let mut chunk_extra = apply_result_to_chunk_extra(main_apply_result, &chunk_header);
    if chunk_extra.state_root() != &state_witness.main_state_transition.post_state_root {
        // This is an early check, it's not for correctness, only for better
        // error reporting in case of an invalid state witness due to a bug.
        // Only the final state root check against the chunk header is required.
        return Err(Error::InvalidChunkStateWitness(format!(
            "Post state root {:?} for main transition does not match expected post state root {:?}",
            chunk_extra.state_root(),
            state_witness.main_state_transition.post_state_root,
        )));
    }

    for (block, transition) in pre_validation_output
        .implicit_transition_params
        .into_iter()
        .zip(state_witness.implicit_transitions.into_iter())
    {
        let block_hash = block.block_hash;
        let old_chunk_data = OldChunkData {
            prev_chunk_extra: chunk_extra.clone(),
            resharding_state_roots: None,
            block,
            storage_context: StorageContext {
                storage_data_source: StorageDataSource::Recorded(PartialStorage {
                    nodes: transition.base_state,
                }),
                state_patch: Default::default(),
                record_storage: false,
            },
        };
        let OldChunkResult { apply_result, .. } = apply_old_chunk(
            &span,
            old_chunk_data,
            ShardContext {
                // Consider other shard uid in case of resharding.
                shard_uid,
                cares_about_shard_this_epoch: true,
                will_shard_layout_change: false,
                should_apply_chunk: false,
                need_to_reshard: false,
            },
            runtime_adapter,
            epoch_manager,
        )?;
        *chunk_extra.state_root_mut() = apply_result.new_root;
        if chunk_extra.state_root() != &transition.post_state_root {
            // This is an early check, it's not for correctness, only for better
            // error reporting in case of an invalid state witness due to a bug.
            // Only the final state root check against the chunk header is required.
            return Err(Error::InvalidChunkStateWitness(format!(
                "Post state root {:?} for implicit transition at block {:?}, does not match expected state root {:?}",
                chunk_extra.state_root(), block_hash, transition.post_state_root
            )));
        }
    }

    // Finally, verify that the newly proposed chunk matches everything we have computed.
    let outgoing_receipts_hashes = {
        let shard_layout = epoch_manager
            .get_shard_layout_from_prev_block(state_witness.chunk_header.prev_block_hash())?;
        Chain::build_receipts_hashes(&outgoing_receipts, &shard_layout)
    };
    let (outgoing_receipts_root, _) = merklize(&outgoing_receipts_hashes);
    validate_chunk_with_chunk_extra_and_receipts_root(
        &chunk_extra,
        &state_witness.chunk_header,
        &outgoing_receipts_root,
    )?;

    // Before we're done we have one last thing to do: verify that the proposed transactions
    // are valid.
    // TODO(#9292): Not sure how to do this.
    Ok(())
}

#[allow(unused)]
fn apply_result_to_chunk_extra(
    apply_result: ApplyChunkResult,
    chunk: &ShardChunkHeader,
) -> ChunkExtra {
    let (outcome_root, _) = ApplyChunkResult::compute_outcomes_proof(&apply_result.outcomes);
    ChunkExtra::new(
        &apply_result.new_root,
        outcome_root,
        apply_result.validator_proposals,
        apply_result.total_gas_burnt,
        chunk.gas_limit(),
        apply_result.total_balance_burnt,
    )
}

impl Client {
    /// Responds to a network request to verify a `ChunkStateWitness`, which is
    /// sent by chunk producers after they produce a chunk.
    pub fn process_chunk_state_witness(&mut self, witness: ChunkStateWitness) -> Result<(), Error> {
        // TODO(#10265): If the previous block does not exist, we should
        // queue this (similar to orphans) to retry later.
        self.chunk_validator.start_validating_chunk(witness, self.chain.chain_store())
    }

    /// Collect state transition data necessary to produce state witness for
    /// `chunk_header`.
    fn collect_state_transition_data(
        &mut self,
        chunk_header: &ShardChunkHeader,
        prev_chunk_header: ShardChunkHeader,
    ) -> Result<(ChunkStateTransition, Vec<ChunkStateTransition>, CryptoHash), Error> {
        let shard_id = chunk_header.shard_id();
        let epoch_id =
            self.epoch_manager.get_epoch_id_from_prev_block(chunk_header.prev_block_hash())?;
        let shard_uid = self.epoch_manager.shard_id_to_uid(shard_id, &epoch_id)?;
        let prev_chunk_height_included = prev_chunk_header.height_included();

        let mut prev_blocks = self.chain.get_blocks_until_height(
            *chunk_header.prev_block_hash(),
            prev_chunk_height_included,
            true,
        )?;
        prev_blocks.reverse();
        let (main_block, implicit_blocks) = prev_blocks.split_first().unwrap();
        let store = self.chain.chain_store().store();
        let StoredChunkStateTransitionData { base_state, receipts_hash } = store
            .get_ser(
                near_store::DBCol::StateTransitionData,
                &near_primitives::utils::get_block_shard_id(main_block, shard_id),
            )?
            .ok_or(Error::Other(format!(
                "Missing state proof for block {main_block} and shard {shard_id}"
            )))?;
        let main_transition = ChunkStateTransition {
            block_hash: *main_block,
            base_state,
            post_state_root: *self.chain.get_chunk_extra(main_block, &shard_uid)?.state_root(),
        };
        let mut implicit_transitions = vec![];
        for block_hash in implicit_blocks {
            let StoredChunkStateTransitionData { base_state, .. } = store
                .get_ser(
                    near_store::DBCol::StateTransitionData,
                    &near_primitives::utils::get_block_shard_id(block_hash, shard_id),
                )?
                .ok_or(Error::Other(format!(
                    "Missing state proof for block {block_hash} and shard {shard_id}"
                )))?;
            implicit_transitions.push(ChunkStateTransition {
                block_hash: *block_hash,
                base_state,
                post_state_root: *self.chain.get_chunk_extra(block_hash, &shard_uid)?.state_root(),
            });
        }

        // TODO(#10265): If the previous block does not exist, we should
        // queue this (similar to orphans) to retry later.

        Ok((main_transition, implicit_transitions, receipts_hash))
    }

    /// State witness proves the execution of receipts proposed by `prev_chunk`.
    /// This function collects all incoming receipts for `prev_chunk`, along with the proofs
    /// that those receipts really originate from the right chunks.
    /// TODO(resharding): `get_incoming_receipts_for_shard` generates invalid proofs on resharding
    /// boundaries, because it removes the receipts that target the other half of a split shard,
    /// which makes the proof invalid. We need to collect the original proof and later, after verifcation,
    /// filter it to remove the receipts that were meant for the other half of the split shard.
    fn collect_source_receipt_proofs(
        &self,
        prev_block: &Block,
        prev_chunk_header: &ShardChunkHeader,
    ) -> Result<HashMap<ChunkHash, ReceiptProof>, Error> {
        // Find the first block that included `prev_chunk`.
        // Incoming receipts were generated by the blocks before this one.
        let prev_chunk_original_block: Cow<BlockHeader> = {
            let mut cur_block: Cow<BlockHeader> = Cow::Borrowed(prev_block.header());
            loop {
                if prev_chunk_header.is_new_chunk(cur_block.height()) {
                    break cur_block;
                }

                cur_block =
                    Cow::Owned(self.chain.chain_store().get_block_header(cur_block.prev_hash())?);
            }
        };

        // Get the last block that contained `prev_prev_chunk` (the chunk before `prev_chunk`).
        // We are interested in all incoming receipts that weren't handled by `prev_prev_chunk`.
        let prev_prev_chunk_block =
            self.chain.chain_store().get_block(prev_chunk_original_block.prev_hash())?;
        // Find the header of the chunk before `prev_chunk`
        let prev_prev_chunk_header = Chain::get_prev_chunk_header(
            self.epoch_manager.as_ref(),
            &prev_prev_chunk_block,
            prev_chunk_header.shard_id(),
        )?;

        // Fetch all incoming receipts for `prev_chunk`.
        // They will be between `prev_prev_chunk.height_included`` (first block containing `prev_prev_chunk`)
        // and `prev_chunk_original_block`
        let incoming_receipt_proofs = self.chain.chain_store().get_incoming_receipts_for_shard(
            self.epoch_manager.as_ref(),
            prev_chunk_header.shard_id(),
            *prev_chunk_original_block.hash(),
            prev_prev_chunk_header.height_included(),
        )?;

        // Convert to the right format (from [block_hash -> Vec<ReceiptProof>] to [chunk_hash -> ReceiptProof])
        let mut source_receipt_proofs = HashMap::new();
        for receipt_proof_response in incoming_receipt_proofs {
            let from_block = self.chain.chain_store().get_block(&receipt_proof_response.0)?;
            for proof in receipt_proof_response.1.iter() {
                let from_shard_id: usize = proof
                    .1
                    .from_shard_id
                    .try_into()
                    .map_err(|_| Error::Other("Couldn't convert u64 to usize!".into()))?;
                let from_chunk_hash = from_block
                    .chunks()
                    .get(from_shard_id)
                    .ok_or_else(|| Error::InvalidShardId(proof.1.from_shard_id))?
                    .chunk_hash();
                let insert_res =
                    source_receipt_proofs.insert(from_chunk_hash.clone(), proof.clone());
                if insert_res.is_some() {
                    // There should be exactly one chunk proof for every chunk
                    return Err(Error::Other(format!(
                        "collect_source_receipt_proofs: duplicate receipt proof for chunk {:?}",
                        from_chunk_hash
                    )));
                }
            }
        }
        Ok(source_receipt_proofs)
    }

    /// Distributes the chunk state witness to chunk validators that are
    /// selected to validate this chunk.
    pub fn send_chunk_state_witness_to_chunk_validators(
        &mut self,
        epoch_id: &EpochId,
        prev_block: &Block,
        chunk: &ShardChunk,
    ) -> Result<(), Error> {
        let protocol_version = self.epoch_manager.get_epoch_protocol_version(epoch_id)?;
        if !checked_feature!("stable", ChunkValidation, protocol_version) {
            return Ok(());
        }

        let chunk_header = chunk.cloned_header();
        let prev_chunk_header = Chain::get_prev_chunk_header(
            self.epoch_manager.as_ref(),
            prev_block,
            chunk.shard_id(),
        )?;

        // First chunk after genesis doesn't have to be endorsed.
        if prev_chunk_header.prev_block_hash() == &CryptoHash::default() {
            return Ok(());
        }

        let chunk_validators = self
            .epoch_manager
            .get_chunk_validator_assignments(
                epoch_id,
                chunk_header.shard_id(),
                chunk_header.height_created(),
            )?
            .ordered_chunk_validators();
        let prev_chunk = self.chain.get_chunk(&prev_chunk_header.chunk_hash())?;
        let source_receipt_proofs =
            self.collect_source_receipt_proofs(prev_block, &prev_chunk_header)?;
        let (main_state_transition, implicit_transitions, applied_receipts_hash) =
            self.collect_state_transition_data(&chunk_header, prev_chunk_header)?;
        let witness = ChunkStateWitness {
            chunk_header: chunk_header.clone(),
            main_state_transition,
            source_receipt_proofs,
            transactions: prev_chunk.transactions().to_vec(),
            // (Could also be derived from iterating through the receipts, but
            // that defeats the purpose of this check being a debugging
            // mechanism.)
            applied_receipts_hash,
            implicit_transitions,
            new_transactions: chunk.transactions().to_vec(),
            // TODO(#9292): Derive this during chunk production, during
            // prepare_transactions or the like.
            new_transactions_validation_state: PartialState::default(),
        };
        tracing::debug!(
            target: "chunk_validation",
            "Sending chunk state witness for chunk {:?} to chunk validators {:?}",
            chunk_header.chunk_hash(),
            chunk_validators,
        );
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::ChunkStateWitness(chunk_validators, witness),
        ));
        Ok(())
    }

    /// Function to process an incoming chunk endorsement from chunk validators.
    /// We first verify the chunk endorsement and then store it in a cache.
    /// We would later include the endorsements in the block production.
    pub fn process_chunk_endorsement(
        &mut self,
        endorsement: ChunkEndorsement,
    ) -> Result<(), Error> {
        let chunk_hash = endorsement.chunk_hash();
        let account_id = &endorsement.account_id;

        // If we have already processed this chunk endorsement, return early.
        if self
            .chunk_validator
            .chunk_endorsements
            .get(chunk_hash)
            .is_some_and(|existing_endorsements| existing_endorsements.get(account_id).is_some())
        {
            tracing::debug!(target: "chunk_validation", ?endorsement, "Already received chunk endorsement.");
            return Ok(());
        }

        let chunk_header = self.chain.get_chunk(chunk_hash)?.cloned_header();
        if !self.epoch_manager.verify_chunk_endorsement(&chunk_header, &endorsement)? {
            tracing::error!(target: "chunk_validation", ?endorsement, "Invalid chunk endorsement.");
            return Err(Error::InvalidChunkEndorsement);
        }

        // If we are the current block producer, we store the chunk endorsement for each chunk which
        // would later be used during block production to check whether to include the chunk or not.
        // TODO(stateless_validation): It's possible for a malicious validator to send endorsements
        // for 100 unique chunks thus pushing out current valid endorsements from our cache.
        // Maybe add check to ensure we don't accept endorsements from chunks already included in some block?
        // Maybe add check to ensure we don't accept endorsements from chunks that have too old height_created?
        tracing::debug!(target: "chunk_validation", ?endorsement, "Received and saved chunk endorsement.");
        self.chunk_validator
            .chunk_endorsements
            .get_or_insert(chunk_hash.clone(), || HashMap::new());
        let chunk_endorsements =
            self.chunk_validator.chunk_endorsements.get_mut(chunk_hash).unwrap();
        chunk_endorsements.insert(account_id.clone(), endorsement);

        Ok(())
    }
}
