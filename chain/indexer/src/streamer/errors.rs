use near_async::messaging::AsyncSendError;
use near_primitives::hash::CryptoHash;

/// Error occurs in case of failed data fetch
#[derive(Debug)]
pub enum FailedToFetchData {
    String(String),
    /// A delayed local receipt referenced by an execution outcome could not be
    /// reconstructed by scanning previous blocks, so the streamer message would
    /// be incomplete. For a regular indexer this is fatal, but best-effort
    /// consumers can opt to skip the block via `IndexerConfig::skip_broken_blocks`.
    LocalReceiptNotFound(CryptoHash),
}

impl From<AsyncSendError> for FailedToFetchData {
    fn from(async_send_error: AsyncSendError) -> Self {
        match async_send_error {
            AsyncSendError::Closed => FailedToFetchData::String("Actor is closed".to_string()),
            AsyncSendError::Timeout => {
                FailedToFetchData::String("Actor send timed out".to_string())
            }
            AsyncSendError::Dropped => {
                FailedToFetchData::String("Actor send was dropped".to_string())
            }
        }
    }
}
