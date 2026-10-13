pub mod delivery;
pub mod introspection;
mod output;
pub(crate) mod service;
pub(crate) mod state;
#[cfg(test)]
#[path = "../tests/fixtures/xerial_decoder.rs"]
mod test_xerial_decoder;

pub use delivery::FirehoseDeliveryImpl;
pub use service::FirehoseService;
pub use state::{
    DeliveryStream, FirehoseAccounts, FirehoseSnapshot, S3Destination, SharedFirehoseState,
    FIREHOSE_SNAPSHOT_SCHEMA_VERSION,
};

/// Validate an S3/ExtendedS3 destination's `FileExtension` exactly as
/// `CreateDeliveryStream` and `UpdateDestination` do, for provisioning paths
/// (CloudFormation) that build destinations outside this crate.
pub fn validate_s3_file_extension(
    destination: &serde_json::Value,
) -> Result<(), fakecloud_core::service::AwsServiceError> {
    service::validate_file_extension(destination)
}
