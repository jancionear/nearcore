use near_store::Store;

use super::OrdinalInconsistency;

pub fn repair_ordinal_inconsistencies(
    store: &Store,
    inconsistencies: &[OrdinalInconsistency],
) -> anyhow::Result<()> {
    todo!();
}
