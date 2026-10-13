use std::sync::Arc;

use fakecloud_core::delivery::FirehoseDelivery;
use fakecloud_persistence::S3Store;
use fakecloud_s3::SharedS3State;

use crate::engine::FirehoseEngine;
use crate::state::SharedFirehoseState;
use bytes::Bytes;

/// Lets non-Firehose services route records into a delivery stream
/// without taking a direct dep on the firehose crate. CloudWatch Logs
/// subscription filters use this for `arn:aws:firehose:` destinations.
pub struct FirehoseDeliveryImpl {
    engine: Arc<FirehoseEngine>,
}

impl FirehoseDeliveryImpl {
    /// Construct an inert, S3-backed adapter with its own buffering engine.
    ///
    /// Embedding callers must obtain [`Self::delivery_engine`], explicitly call
    /// [`FirehoseEngine::start`] inside a Tokio runtime for ordinary delivery,
    /// and await [`FirehoseEngine::shutdown`] after producers stop for the final
    /// drain. Record handoffs acknowledge admission, not S3 publication. Neither
    /// construction, store registration, nor handoff starts an implicit worker.
    /// Pending buffers are process-local; dropping the adapter/engine without a
    /// final drain loses them. Callers sharing an engine with a Firehose service
    /// should use [`Self::with_engine`] instead and manage that engine once.
    pub fn new(state: SharedFirehoseState, s3: SharedS3State) -> Self {
        let engine = Arc::new(FirehoseEngine::new(state));
        engine.set_s3(s3);
        Self::with_engine(engine)
    }

    /// Use an externally owned engine, sharing its admission and lifecycle.
    pub fn with_engine(engine: Arc<FirehoseEngine>) -> Self {
        Self { engine }
    }

    /// Return the engine used by every trait handoff, for explicit start/drain.
    /// This accessor does not start a worker. A final drain can also be awaited
    /// without starting ordinary delivery first.
    pub fn delivery_engine(&self) -> Arc<FirehoseEngine> {
        self.engine.clone()
    }

    /// Wire the durable S3 store after construction. The delivery hook is built
    /// during startup before the S3 store exists, so the store is set later so
    /// records delivered via the CloudWatch-Logs subscription path are written
    /// through to disk and survive a restart.
    pub fn set_s3_store(&self, store: Arc<dyn S3Store>) {
        self.engine.set_s3_store(store);
    }
}

impl FirehoseDelivery for FirehoseDeliveryImpl {
    fn put_record(&self, delivery_stream_arn: &str, data: &[u8]) {
        let Some((account, region, stream_name)) = parse_arn(delivery_stream_arn) else {
            tracing::warn!(arn = %delivery_stream_arn, "invalid firehose ARN");
            return;
        };
        if let Err(err) = self.engine.enqueue(
            &account,
            &region,
            &stream_name,
            vec![Bytes::copy_from_slice(data)],
        ) {
            tracing::warn!(
                arn = %delivery_stream_arn,
                error = ?err,
                "firehose delivery failed"
            );
        }
    }
}

fn parse_arn(arn: &str) -> Option<(String, String, String)> {
    // arn:aws:firehose:<region>:<account>:deliverystream/<name>
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    if parts.len() < 6 || parts[0] != "arn" || parts[2] != "firehose" {
        return None;
    }
    let stream_name = parts[5].strip_prefix("deliverystream/")?;
    Some((
        parts[4].to_string(),
        parts[3].to_string(),
        stream_name.to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_arn_extracts_components() {
        let parsed = parse_arn("arn:aws:firehose:us-east-1:123456789012:deliverystream/logs-to-s3");
        assert_eq!(
            parsed,
            Some((
                "123456789012".to_string(),
                "us-east-1".to_string(),
                "logs-to-s3".to_string(),
            ))
        );
    }

    #[test]
    fn parse_arn_rejects_non_firehose_arns() {
        assert!(parse_arn("arn:aws:s3:::bucket").is_none());
        assert!(parse_arn("not-an-arn").is_none());
        assert!(parse_arn("arn:aws:firehose:us-east-1:111:stream/foo").is_none());
    }
}
