use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use fakecloud_aws::arn::Arn;
use http::StatusCode;
use parking_lot::RwLock;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::{S3Store, SnapshotStore};
use fakecloud_s3::delivery::{
    put_object_via_store_with_options, DeliveryObjectOptions, DeliveryObjectRequest,
};
use fakecloud_s3::SharedS3State;

use crate::output::{
    append_delimiter_enabled, encode_records, object_key, Compression, OutputConfig,
};
use crate::state::{
    DeliveryStream, EncryptionConfig, FirehoseAccounts, FirehoseSnapshot, S3Destination,
    SharedFirehoseState, FIREHOSE_SNAPSHOT_SCHEMA_VERSION,
};

/// Actions that mutate persisted Firehose state and therefore must trigger a
/// snapshot write. PutRecord/PutRecordBatch deliver to S3 (persisted by the S3
/// store) but do not change Firehose's own state, so they are excluded.
const MUTATING_ACTIONS: &[&str] = &[
    "CreateDeliveryStream",
    "DeleteDeliveryStream",
    "TagDeliveryStream",
    "UntagDeliveryStream",
    "UpdateDestination",
    "StartDeliveryStreamEncryption",
    "StopDeliveryStreamEncryption",
];

const SUPPORTED_ACTIONS: &[&str] = &[
    "CreateDeliveryStream",
    "DescribeDeliveryStream",
    "ListDeliveryStreams",
    "DeleteDeliveryStream",
    "PutRecord",
    "PutRecordBatch",
    "TagDeliveryStream",
    "UntagDeliveryStream",
    "ListTagsForDeliveryStream",
    "UpdateDestination",
    "StartDeliveryStreamEncryption",
    "StopDeliveryStreamEncryption",
];

pub struct FirehoseService {
    state: SharedFirehoseState,
    s3: Option<SharedS3State>,
    /// Durable S3 backing store. S3 durability is write-through to this store
    /// (there is no `s3_state` snapshot; on boot S3 is rebuilt from the store),
    /// so records delivered to an S3 destination must be routed through it or
    /// they are lost on restart. Behind a lock so the CloudWatch-Logs delivery
    /// hook (built before the S3 store exists) can be wired up after the fact
    /// via `set_s3_store`.
    s3_store: RwLock<Option<Arc<dyn S3Store>>>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl FirehoseService {
    pub fn new(state: SharedFirehoseState) -> Self {
        Self {
            state,
            s3: None,
            s3_store: RwLock::new(None),
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_s3(mut self, s3: SharedS3State) -> Self {
        self.s3 = Some(s3);
        self
    }

    /// Builder form of [`Self::set_s3_store`], for the public service which is
    /// constructed after the S3 store already exists.
    pub fn with_s3_store(self, store: Arc<dyn S3Store>) -> Self {
        self.set_s3_store(store);
        self
    }

    /// Wire the durable S3 store after construction. Used by
    /// `FirehoseDeliveryImpl`, whose delivery hook is built before the S3 store
    /// is available during server startup.
    pub fn set_s3_store(&self, store: Arc<dyn S3Store>) {
        *self.s3_store.write() = Some(store);
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    pub fn shared_state(&self) -> SharedFirehoseState {
        Arc::clone(&self.state)
    }

    /// Persist current state as a snapshot. Held across the
    /// clone-serialize-write sequence to prevent stale-last writes, with serde
    /// + file I/O offloaded to the blocking pool.
    async fn save_snapshot(&self) {
        save_firehose_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current Firehose state when invoked, or
    /// `None` in memory mode (no snapshot store). The CloudFormation provisioner
    /// mutates `state` directly and uses this to write a CFN-provisioned
    /// resource through to disk, the same way a direct mutating API call would.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_firehose_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }
}

/// Persist the current Firehose state as a snapshot. Offloads the serde +
/// blocking file write to the Tokio blocking pool. Noop when `store` is `None`
/// (memory mode). Shared by `FirehoseService::save_snapshot` and the
/// CloudFormation provisioner's post-provision persist hook so both route
/// through the same serialize-and-write path.
pub async fn save_firehose_snapshot(
    state: &SharedFirehoseState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = FirehoseSnapshot {
        schema_version: FIREHOSE_SNAPSHOT_SCHEMA_VERSION,
        accounts: Some(state.read().clone()),
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write firehose snapshot"),
        Err(err) => tracing::error!(%err, "firehose snapshot task panicked"),
    }
}

impl Default for FirehoseService {
    fn default() -> Self {
        Self::new(Arc::new(RwLock::new(FirehoseAccounts::new())))
    }
}

#[async_trait]
impl AwsService for FirehoseService {
    fn service_name(&self) -> &str {
        "firehose"
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = MUTATING_ACTIONS.contains(&req.action.as_str());
        let result = match req.action.as_str() {
            "CreateDeliveryStream" => self.create_delivery_stream(&req),
            "DescribeDeliveryStream" => self.describe_delivery_stream(&req),
            "ListDeliveryStreams" => self.list_delivery_streams(&req),
            "DeleteDeliveryStream" => self.delete_delivery_stream(&req),
            "PutRecord" => self.put_record(&req),
            "PutRecordBatch" => self.put_record_batch(&req),
            "TagDeliveryStream" => self.tag_delivery_stream(&req),
            "UntagDeliveryStream" => self.untag_delivery_stream(&req),
            "ListTagsForDeliveryStream" => self.list_tags_for_delivery_stream(&req),
            "UpdateDestination" => self.update_destination(&req),
            "StartDeliveryStreamEncryption" => self.start_delivery_stream_encryption(&req),
            "StopDeliveryStreamEncryption" => self.stop_delivery_stream_encryption(&req),
            other => Err(AwsServiceError::action_not_implemented("firehose", other)),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }
}

fn missing(field: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidArgumentException",
        format!("Missing required field: {field}"),
    )
}

/// Non-S3 destination base names. Firehose names each destination's input
/// `<Base>Configuration` (create) / `<Base>Update` (update) and reports it as
/// `<Base>Description`. fakecloud models S3/ExtendedS3 field-by-field; these
/// are round-tripped as raw JSON so DescribeDeliveryStream reflects them.
const EXTRA_DESTINATION_BASES: &[&str] = &[
    "RedshiftDestination",
    "ElasticsearchDestination",
    "AmazonopensearchserviceDestination",
    "AmazonOpenSearchServerlessDestination",
    "SplunkDestination",
    "HttpEndpointDestination",
    "SnowflakeDestination",
    "IcebergDestination",
];

/// Pull any non-S3 destination config/update blocks out of `body`, keyed by
/// their `<Base>Description` field name. `suffix` is "Configuration" (create)
/// or "Update" (update). The HTTP-endpoint destination redacts its access key
/// in the description, mirroring real Firehose.
fn extract_extra_destinations(body: &Value, suffix: &str) -> BTreeMap<String, Value> {
    let mut out = BTreeMap::new();
    for base in EXTRA_DESTINATION_BASES {
        let input_key = format!("{base}{suffix}");
        if let Some(cfg) = body.get(&input_key).filter(|v| v.is_object()) {
            out.insert(format!("{base}Description"), cfg.clone());
        }
    }
    out
}

fn parse_s3_destination(val: &Value) -> Result<Option<S3Destination>, AwsServiceError> {
    if !val.is_object() {
        return Ok(None);
    }
    let Some(role_arn) = val["RoleARN"].as_str().map(|s| s.to_string()) else {
        return Ok(None);
    };
    let Some(bucket_arn) = val["BucketARN"].as_str().map(|s| s.to_string()) else {
        return Ok(None);
    };
    let buf = &val["BufferingHints"];
    let buffering_size_mb = buf["SizeInMBs"].as_i64();
    let buffering_interval_seconds = buf["IntervalInSeconds"].as_i64();
    validate_buffering(buffering_size_mb, buffering_interval_seconds)?;
    validate_file_extension(val)?;
    Ok(Some(S3Destination {
        destination_id: format!("destinationId-{}", Uuid::new_v4()),
        role_arn,
        bucket_arn,
        prefix: val["Prefix"].as_str().map(|s| s.to_string()),
        error_output_prefix: val["ErrorOutputPrefix"].as_str().map(|s| s.to_string()),
        buffering_size_mb,
        buffering_interval_seconds,
        compression_format: val["CompressionFormat"].as_str().map(|s| s.to_string()),
        processing_configuration: val["ProcessingConfiguration"]
            .is_object()
            .then(|| val["ProcessingConfiguration"].clone()),
        data_format_conversion_configuration: val["DataFormatConversionConfiguration"]
            .is_object()
            .then(|| val["DataFormatConversionConfiguration"].clone()),
        cloudwatch_logging_options: val["CloudWatchLoggingOptions"]
            .is_object()
            .then(|| val["CloudWatchLoggingOptions"].clone()),
        custom_time_zone: val["CustomTimeZone"].as_str().map(|s| s.to_string()),
        s3_backup_mode: val["S3BackupMode"].as_str().map(|s| s.to_string()),
        file_extension: val["FileExtension"].as_str().map(|s| s.to_string()),
        dynamic_partitioning_configuration: val["DynamicPartitioningConfiguration"]
            .is_object()
            .then(|| val["DynamicPartitioningConfiguration"].clone()),
        encryption_configuration: val["EncryptionConfiguration"]
            .is_object()
            .then(|| val["EncryptionConfiguration"].clone()),
        // The `S3BackupConfiguration` create block is a full S3 destination
        // config; stored as raw JSON and echoed back as `S3BackupDescription`.
        s3_backup_description: val["S3BackupConfiguration"]
            .is_object()
            .then(|| val["S3BackupConfiguration"].clone()),
    }))
}

fn invalid_argument(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidArgumentException", msg)
}

pub(crate) fn validate_file_extension(value: &Value) -> Result<(), AwsServiceError> {
    let Some(extension) = value.get("FileExtension") else {
        return Ok(());
    };
    let extension = extension
        .as_str()
        .ok_or_else(|| invalid_argument("FileExtension must be a string"))?;
    // Equivalent to ^(|\.[0-9a-z!\-_.*'()]+)$ without a regex dependency.
    if extension.len() > 128
        || (!extension.is_empty()
            && (!extension.starts_with('.')
                || extension.len() < 2
                || !extension[1..].bytes().all(|c| {
                    c.is_ascii_digit()
                        || c.is_ascii_lowercase()
                        || matches!(c, b'!' | b'-' | b'_' | b'.' | b'*' | b'\'' | b'(' | b')')
                })))
    {
        return Err(invalid_argument("FileExtension must be empty or a dot followed by lowercase letters, digits, or !-_.*'(), with at most 128 characters"));
    }
    Ok(())
}

/// Apply a partial `*S3DestinationUpdate` block onto an existing destination.
/// Every field in the `*Update` shape is optional, so only the members the
/// caller supplied are changed; the destination's stable `DestinationId` is
/// preserved. Buffering values are validated when present.
fn apply_s3_update(dest: &mut S3Destination, val: &Value) -> Result<(), AwsServiceError> {
    let buf = &val["BufferingHints"];
    let size = buf["SizeInMBs"].as_i64();
    let interval = buf["IntervalInSeconds"].as_i64();
    validate_buffering(size, interval)?;
    validate_file_extension(val)?;

    if let Some(v) = val["RoleARN"].as_str() {
        dest.role_arn = v.to_string();
    }
    if let Some(v) = val["BucketARN"].as_str() {
        dest.bucket_arn = v.to_string();
    }
    if let Some(v) = val["Prefix"].as_str() {
        dest.prefix = Some(v.to_string());
    }
    if let Some(v) = val["ErrorOutputPrefix"].as_str() {
        dest.error_output_prefix = Some(v.to_string());
    }
    if size.is_some() {
        dest.buffering_size_mb = size;
    }
    if interval.is_some() {
        dest.buffering_interval_seconds = interval;
    }
    if let Some(v) = val["CompressionFormat"].as_str() {
        dest.compression_format = Some(v.to_string());
    }
    if val["ProcessingConfiguration"].is_object() {
        dest.processing_configuration = Some(val["ProcessingConfiguration"].clone());
    }
    if val["DataFormatConversionConfiguration"].is_object() {
        dest.data_format_conversion_configuration =
            Some(val["DataFormatConversionConfiguration"].clone());
    }
    if val["CloudWatchLoggingOptions"].is_object() {
        dest.cloudwatch_logging_options = Some(val["CloudWatchLoggingOptions"].clone());
    }
    if let Some(v) = val["CustomTimeZone"].as_str() {
        dest.custom_time_zone = Some(v.to_string());
    }
    if let Some(v) = val["S3BackupMode"].as_str() {
        dest.s3_backup_mode = Some(v.to_string());
    }
    if let Some(v) = val["FileExtension"].as_str() {
        dest.file_extension = Some(v.to_string());
    }
    if val["DynamicPartitioningConfiguration"].is_object() {
        dest.dynamic_partitioning_configuration =
            Some(val["DynamicPartitioningConfiguration"].clone());
    }
    if val["EncryptionConfiguration"].is_object() {
        dest.encryption_configuration = Some(val["EncryptionConfiguration"].clone());
    }
    // `S3BackupUpdate` is an all-optional S3 destination update; merge it field-
    // wise onto the stored backup description rather than replacing it.
    if val["S3BackupUpdate"].is_object() {
        match dest.s3_backup_description.as_mut() {
            Some(existing) => merge_json(existing, &val["S3BackupUpdate"]),
            None => dest.s3_backup_description = Some(val["S3BackupUpdate"].clone()),
        }
    }
    Ok(())
}

/// Recursively merge `update` into `target`: for every key present in `update`,
/// objects recurse and all other values overwrite; keys absent from `update`
/// are left untouched. Firehose's `*Update` shapes are all-optional with merge
/// semantics, so a partial update must not drop the fields the caller omitted.
fn merge_json(target: &mut Value, update: &Value) {
    match (target, update) {
        (Value::Object(tgt), Value::Object(upd)) => {
            for (k, v) in upd {
                merge_json(tgt.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
        (tgt, upd) => *tgt = upd.clone(),
    }
}

/// Increment a Firehose delivery-stream version id. AWS version ids are
/// numeric strings that advance by one on every successful configuration
/// change (UpdateDestination, Start/StopEncryption, ...).
fn next_version_id(current: &str) -> String {
    current
        .parse::<u64>()
        .map(|n| (n + 1).to_string())
        .unwrap_or_else(|_| current.to_string())
}

/// Validate a delivery-stream name against the Firehose model constraints:
/// length 1..=64 and pattern `^[a-zA-Z0-9_.-]+$`. AWS rejects violations with
/// an InvalidArgumentException before any resource lookup.
fn validate_stream_name(name: &str) -> Result<(), AwsServiceError> {
    if name.is_empty() || name.len() > 64 {
        return Err(invalid_argument(format!(
            "DeliveryStreamName must be between 1 and 64 characters, got {}",
            name.len()
        )));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
    {
        return Err(invalid_argument(
            "DeliveryStreamName may only contain letters, digits, '_', '.' and '-'",
        ));
    }
    Ok(())
}

const DELIVERY_STREAM_TYPES: &[&str] = &[
    "DirectPut",
    "KinesisStreamAsSource",
    "MSKAsSource",
    "DatabaseAsSource",
];

/// AWS limits per Firehose docs: SizeInMBs in [1,128], IntervalInSeconds
/// either 0 (immediate) or in [60,900].
fn validate_buffering(
    size_mb: Option<i64>,
    interval_s: Option<i64>,
) -> Result<(), AwsServiceError> {
    if let Some(s) = size_mb {
        if !(1..=128).contains(&s) {
            return Err(invalid_argument(format!(
                "BufferingHints.SizeInMBs must be between 1 and 128, got {s}"
            )));
        }
    }
    if let Some(i) = interval_s {
        if i != 0 && !(60..=900).contains(&i) {
            return Err(invalid_argument(format!(
                "BufferingHints.IntervalInSeconds must be 0 or between 60 and 900, got {i}"
            )));
        }
    }
    Ok(())
}

fn s3_destination_json(dest: &S3Destination) -> Value {
    let mut buf = json!({});
    if let Some(s) = dest.buffering_size_mb {
        buf["SizeInMBs"] = json!(s);
    }
    if let Some(i) = dest.buffering_interval_seconds {
        buf["IntervalInSeconds"] = json!(i);
    }
    let mut s3 = json!({
        "RoleARN": dest.role_arn,
        "BucketARN": dest.bucket_arn,
        "BufferingHints": buf,
        "CompressionFormat": dest.compression_format.clone().unwrap_or_else(|| "UNCOMPRESSED".to_string()),
    });
    if let Some(ref p) = dest.prefix {
        s3["Prefix"] = json!(p);
    }
    if let Some(ref p) = dest.error_output_prefix {
        s3["ErrorOutputPrefix"] = json!(p);
    }
    // ProcessingConfiguration / DataFormatConversionConfiguration are only
    // valid on the ExtendedS3 shape, not the legacy S3DestinationDescription.
    let mut extended = s3.clone();
    if let Some(ref pc) = dest.processing_configuration {
        extended["ProcessingConfiguration"] = pc.clone();
    }
    if let Some(ref dfc) = dest.data_format_conversion_configuration {
        extended["DataFormatConversionConfiguration"] = dfc.clone();
    }
    // AWS always reports these on the ExtendedS3 description, defaulting them
    // when the caller omits the values. The Terraform resource reads the
    // CloudWatch logging block, custom_time_zone, and s3_backup_mode
    // unconditionally; an empty default block leaves them "not found" / "".
    extended["CloudWatchLoggingOptions"] = dest
        .cloudwatch_logging_options
        .clone()
        .unwrap_or_else(|| json!({ "Enabled": false }));
    extended["CustomTimeZone"] = json!(dest.custom_time_zone.as_deref().unwrap_or("UTC"));
    extended["S3BackupMode"] = json!(dest.s3_backup_mode.as_deref().unwrap_or("Disabled"));
    if let Some(ref ext) = dest.file_extension {
        extended["FileExtension"] = json!(ext);
    }
    // ExtendedS3-only descriptions: dynamic partitioning, delivered-object SSE
    // config, and the nested S3 backup destination description.
    if let Some(ref dp) = dest.dynamic_partitioning_configuration {
        extended["DynamicPartitioningConfiguration"] = dp.clone();
    }
    if let Some(ref enc) = dest.encryption_configuration {
        extended["EncryptionConfiguration"] = enc.clone();
    }
    if let Some(ref backup) = dest.s3_backup_description {
        extended["S3BackupDescription"] = backup.clone();
    }
    json!({
        "DestinationId": dest.destination_id,
        "S3DestinationDescription": s3,
        "ExtendedS3DestinationDescription": extended,
    })
}

/// Parse a `DeliveryStreamEncryptionConfigurationInput` (shared by
/// CreateDeliveryStream and StartDeliveryStreamEncryption) into the ENABLED
/// SSE config it requests. `KeyType` defaults to `AWS_OWNED_CMK`; a
/// `CUSTOMER_MANAGED_CMK` requires `KeyARN`.
fn parse_encryption_input(config: &Value) -> Result<EncryptionConfig, AwsServiceError> {
    let key_type = config["KeyType"]
        .as_str()
        .unwrap_or("AWS_OWNED_CMK")
        .to_string();
    if key_type != "AWS_OWNED_CMK" && key_type != "CUSTOMER_MANAGED_CMK" {
        return Err(invalid_argument(format!(
            "KeyType must be AWS_OWNED_CMK or CUSTOMER_MANAGED_CMK, got {key_type}"
        )));
    }
    let key_arn = if key_type == "CUSTOMER_MANAGED_CMK" {
        let arn = config["KeyARN"].as_str().ok_or_else(|| {
            invalid_argument("KeyARN is required when KeyType is CUSTOMER_MANAGED_CMK")
        })?;
        Some(arn.to_string())
    } else {
        None
    };
    Ok(EncryptionConfig {
        status: "ENABLED".to_string(),
        key_type: Some(key_type),
        key_arn,
    })
}

fn encryption_config_json(enc: &EncryptionConfig) -> Value {
    let mut v = json!({ "Status": enc.status });
    if let Some(kt) = &enc.key_type {
        v["KeyType"] = json!(kt);
    }
    if let Some(ka) = &enc.key_arn {
        v["KeyARN"] = json!(ka);
    }
    v
}

fn arn_for(region: &str, account: &str, name: &str) -> String {
    Arn::regional(
        "firehose",
        region,
        account,
        &format!("deliverystream/{name}"),
    )
    .to_string()
}

fn bucket_name_from_arn(arn: &str) -> Option<&str> {
    fakecloud_aws::arn::arn_resource(arn, "s3")?.strip_prefix("::")
}

impl FirehoseService {
    fn create_delivery_stream(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["DeliveryStreamName"]
            .as_str()
            .ok_or_else(|| missing("DeliveryStreamName"))?
            .to_string();
        validate_stream_name(&name)?;
        let stream_type = body["DeliveryStreamType"]
            .as_str()
            .unwrap_or("DirectPut")
            .to_string();
        if !DELIVERY_STREAM_TYPES.contains(&stream_type.as_str()) {
            return Err(invalid_argument(format!(
                "Invalid DeliveryStreamType: {stream_type}"
            )));
        }

        let s3_dest = match parse_s3_destination(&body["S3DestinationConfiguration"])? {
            Some(d) => Some(d),
            None => parse_s3_destination(&body["ExtendedS3DestinationConfiguration"])?,
        };
        let extra_destinations = extract_extra_destinations(&body, "Configuration");

        // Create-time Tags must be returned by ListTagsForDeliveryStream without
        // a follow-up TagDeliveryStream call (bug-hunt).
        let mut create_tags = BTreeMap::new();
        if let Some(arr) = body["Tags"].as_array() {
            for t in arr {
                if let (Some(k), Some(v)) = (t["Key"].as_str(), t["Value"].as_str()) {
                    create_tags.insert(k.to_string(), v.to_string());
                }
            }
        }

        // Create-time SSE: same semantics as a follow-up
        // StartDeliveryStreamEncryption.
        let encryption = match &body["DeliveryStreamEncryptionConfigurationInput"] {
            Value::Null => None,
            config => Some(parse_encryption_input(config)?),
        };

        let now = Utc::now();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let streams = state.streams_mut(&req.region);
        if streams.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceInUseException",
                format!("DeliveryStream {name} already exists"),
            ));
        }
        let arn = arn_for(&req.region, &req.account_id, &name);
        let stream = DeliveryStream {
            name: name.clone(),
            arn: arn.clone(),
            status: "ACTIVE".to_string(),
            stream_type,
            created_at: now,
            last_update: now,
            version_id: "1".to_string(),
            destination: s3_dest,
            tags: create_tags,
            encryption,
            extra_destinations,
        };
        streams.insert(name, stream);
        Ok(AwsResponse::ok_json(json!({
            "DeliveryStreamARN": arn,
        })))
    }

    fn describe_delivery_stream(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["DeliveryStreamName"]
            .as_str()
            .ok_or_else(|| missing("DeliveryStreamName"))?;
        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(|| not_found(name))?;
        let stream = state
            .streams(&req.region)
            .and_then(|s| s.get(name))
            .ok_or_else(|| not_found(name))?;

        // A delivery stream reports a single destination object carrying every
        // configured destination description. Start from the S3 description (if
        // any) and fold in the non-S3 descriptions so a Redshift/OpenSearch/
        // Splunk/HTTP destination round-trips instead of vanishing.
        let destinations: Vec<Value> =
            if stream.destination.is_none() && stream.extra_destinations.is_empty() {
                Vec::new()
            } else {
                let mut dest = stream
                    .destination
                    .as_ref()
                    .map(s3_destination_json)
                    .unwrap_or_else(|| json!({ "DestinationId": "destinationId-000000000001" }));
                for (key, value) in &stream.extra_destinations {
                    dest[key] = value.clone();
                }
                vec![dest]
            };

        let mut description = json!({
            "DeliveryStreamName": stream.name,
            "DeliveryStreamARN": stream.arn,
            "DeliveryStreamStatus": stream.status,
            "DeliveryStreamType": stream.stream_type,
            "VersionId": stream.version_id,
            "CreateTimestamp": stream.created_at.timestamp() as f64,
            "LastUpdateTimestamp": stream.last_update.timestamp() as f64,
            "Destinations": destinations,
            "HasMoreDestinations": false,
        });
        if let Some(enc) = &stream.encryption {
            description["DeliveryStreamEncryptionConfiguration"] = encryption_config_json(enc);
        }

        Ok(AwsResponse::ok_json(json!({
            "DeliveryStreamDescription": description,
        })))
    }

    fn list_delivery_streams(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let limit = match body["Limit"].as_i64() {
            Some(l) => {
                if !(1..=10000).contains(&l) {
                    return Err(invalid_argument(format!(
                        "Limit must be between 1 and 10000, got {l}"
                    )));
                }
                l as usize
            }
            None => 10,
        };
        let exclusive_start = body["ExclusiveStartDeliveryStreamName"]
            .as_str()
            .map(|s| s.to_string());
        if let Some(ref start) = exclusive_start {
            validate_stream_name(start)?;
        }
        let type_filter = body["DeliveryStreamType"].as_str().map(|s| s.to_string());
        if let Some(ref t) = type_filter {
            if !DELIVERY_STREAM_TYPES.contains(&t.as_str()) {
                return Err(invalid_argument(format!("Invalid DeliveryStreamType: {t}")));
            }
        }

        let accounts = self.state.read();
        let names: Vec<String> = accounts
            .get(&req.account_id)
            .and_then(|s| s.streams(&req.region))
            .map(|streams| {
                streams
                    .iter()
                    .filter(|(n, stream)| {
                        if let Some(ref start) = exclusive_start {
                            if n.as_str() <= start.as_str() {
                                return false;
                            }
                        }
                        if let Some(ref t) = type_filter {
                            return &stream.stream_type == t;
                        }
                        true
                    })
                    .map(|(n, _)| n.clone())
                    .collect()
            })
            .unwrap_or_default();
        let truncated = names.len() > limit;
        let names: Vec<String> = names.into_iter().take(limit).collect();
        Ok(AwsResponse::ok_json(json!({
            "DeliveryStreamNames": names,
            "HasMoreDeliveryStreams": truncated,
        })))
    }

    fn delete_delivery_stream(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["DeliveryStreamName"]
            .as_str()
            .ok_or_else(|| missing("DeliveryStreamName"))?;
        validate_stream_name(name)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        if state.streams_mut(&req.region).remove(name).is_none() {
            return Err(not_found(name));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn put_record(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["DeliveryStreamName"]
            .as_str()
            .ok_or_else(|| missing("DeliveryStreamName"))?
            .to_string();
        let data_b64 = body["Record"]["Data"]
            .as_str()
            .ok_or_else(|| missing("Record.Data"))?;
        let data = base64::engine::general_purpose::STANDARD
            .decode(data_b64)
            .map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidArgumentException",
                    "Record.Data must be valid base64",
                )
            })?;
        let record_id = format!("{}", Uuid::new_v4());
        self.deliver_records(&req.account_id, &req.region, &name, vec![data])?;
        Ok(AwsResponse::ok_json(json!({
            "RecordId": record_id,
            "Encrypted": false,
        })))
    }

    fn put_record_batch(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["DeliveryStreamName"]
            .as_str()
            .ok_or_else(|| missing("DeliveryStreamName"))?
            .to_string();
        let records = body["Records"]
            .as_array()
            .ok_or_else(|| missing("Records"))?;
        let mut datas = Vec::with_capacity(records.len());
        let mut response_records = Vec::with_capacity(records.len());
        let mut failed = 0;
        for r in records {
            let b64 = r["Data"].as_str();
            match b64.map(|s| base64::engine::general_purpose::STANDARD.decode(s)) {
                Some(Ok(data)) if !data.is_empty() => {
                    datas.push(data);
                    response_records.push(json!({
                        "RecordId": Uuid::new_v4().to_string(),
                    }));
                }
                _ => {
                    failed += 1;
                    response_records.push(json!({
                        "ErrorCode": "InvalidArgumentException",
                        "ErrorMessage": "Record.Data must be a non-empty base64-encoded string",
                    }));
                }
            }
        }
        if !datas.is_empty() {
            self.deliver_records(&req.account_id, &req.region, &name, datas)?;
        } else if failed == records.len() && !records.is_empty() {
            // Validate stream exists even when nothing delivers, so callers
            // get a clear ResourceNotFoundException for typos.
            let accounts = self.state.read();
            let state = accounts
                .get(&req.account_id)
                .ok_or_else(|| not_found(&name))?;
            state
                .streams(&req.region)
                .and_then(|s| s.get(&name))
                .ok_or_else(|| not_found(&name))?;
        }
        Ok(AwsResponse::ok_json(json!({
            "FailedPutCount": failed,
            "Encrypted": false,
            "RequestResponses": response_records,
        })))
    }

    fn tag_delivery_stream(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["DeliveryStreamName"]
            .as_str()
            .ok_or_else(|| missing("DeliveryStreamName"))?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let stream = state
            .streams_mut(&req.region)
            .get_mut(name)
            .ok_or_else(|| not_found(name))?;
        if let Some(arr) = body["Tags"].as_array() {
            for t in arr {
                if let (Some(k), Some(v)) = (t["Key"].as_str(), t["Value"].as_str()) {
                    stream.tags.insert(k.to_string(), v.to_string());
                }
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn untag_delivery_stream(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["DeliveryStreamName"]
            .as_str()
            .ok_or_else(|| missing("DeliveryStreamName"))?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let stream = state
            .streams_mut(&req.region)
            .get_mut(name)
            .ok_or_else(|| not_found(name))?;
        if let Some(arr) = body["TagKeys"].as_array() {
            for k in arr {
                if let Some(s) = k.as_str() {
                    stream.tags.remove(s);
                }
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn list_tags_for_delivery_stream(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["DeliveryStreamName"]
            .as_str()
            .ok_or_else(|| missing("DeliveryStreamName"))?;
        // AWS: Limit @range(1,50) caps the page (default 50);
        // ExclusiveStartTagKey resumes after that key. Tags are ordered by
        // key (BTreeMap iteration). Without this, large tag sets were
        // returned whole with HasMoreTags hardcoded to false (bug-hunt
        // 2026-07-16).
        let limit = match body["Limit"].as_i64() {
            Some(l) => {
                if !(1..=50).contains(&l) {
                    return Err(invalid_argument(format!(
                        "Limit must be between 1 and 50, got {l}"
                    )));
                }
                l as usize
            }
            None => 50,
        };
        let exclusive_start = body["ExclusiveStartTagKey"].as_str();
        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(|| not_found(name))?;
        let stream = state
            .streams(&req.region)
            .and_then(|s| s.get(name))
            .ok_or_else(|| not_found(name))?;
        let remaining: Vec<(&String, &String)> = stream
            .tags
            .iter()
            .filter(|(k, _)| exclusive_start.is_none_or(|start| k.as_str() > start))
            .collect();
        let has_more = remaining.len() > limit;
        let tags: Vec<Value> = remaining
            .into_iter()
            .take(limit)
            .map(|(k, v)| json!({"Key": k, "Value": v}))
            .collect();
        Ok(AwsResponse::ok_json(json!({
            "Tags": tags,
            "HasMoreTags": has_more,
        })))
    }

    fn update_destination(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["DeliveryStreamName"]
            .as_str()
            .ok_or_else(|| missing("DeliveryStreamName"))?;
        // CurrentDeliveryStreamVersionId and DestinationId are both REQUIRED
        // by AWS UpdateDestination.
        let current_version = body["CurrentDeliveryStreamVersionId"]
            .as_str()
            .ok_or_else(|| missing("CurrentDeliveryStreamVersionId"))?;
        let destination_id = body["DestinationId"]
            .as_str()
            .ok_or_else(|| missing("DestinationId"))?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let stream = state
            .streams_mut(&req.region)
            .get_mut(name)
            .ok_or_else(|| not_found(name))?;

        // AWS requires CurrentDeliveryStreamVersionId to match the stream's
        // current version and rejects a mismatch with a concurrent-modification
        // error (this is the optimistic-concurrency guard).
        if current_version != stream.version_id {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ConcurrentModificationException",
                format!(
                    "The current delivery stream version {} does not match \
                     the supplied version {current_version}.",
                    stream.version_id
                ),
            ));
        }

        // DestinationId must identify the stream's current destination.
        let current_dest_id = stream
            .destination
            .as_ref()
            .map(|d| d.destination_id.clone())
            .unwrap_or_else(|| "destinationId-000000000001".to_string());
        if destination_id != current_dest_id {
            return Err(invalid_argument(format!(
                "DestinationId {destination_id} does not match the current \
                 destination {current_dest_id} for delivery stream {name}."
            )));
        }

        // Apply the S3 / ExtendedS3 update. The `*Update` shapes make every
        // member optional, so a partial update (e.g. only BufferingHints) must
        // merge onto the existing destination rather than being dropped, and
        // the destination keeps its stable DestinationId across updates.
        let s3_update = if body["S3DestinationUpdate"].is_object() {
            Some(&body["S3DestinationUpdate"])
        } else if body["ExtendedS3DestinationUpdate"].is_object() {
            Some(&body["ExtendedS3DestinationUpdate"])
        } else {
            None
        };
        if let Some(update) = s3_update {
            match stream.destination.as_mut() {
                Some(existing) => apply_s3_update(existing, update)?,
                // No existing S3 destination: fall back to a full parse
                // (RoleARN + BucketARN required to materialize one).
                None => {
                    if let Some(d) = parse_s3_destination(update)? {
                        stream.destination = Some(d);
                    }
                }
            }
        }

        // Non-S3 destinations (Redshift/OpenSearch/Splunk/HTTP/...) carry
        // all-optional `*Update` shapes. Merge each supplied update field-wise
        // onto the stored `*DestinationDescription` instead of replacing the
        // whole object, so a partial update (e.g. only RetryOptions) does not
        // drop the other fields the caller omitted.
        for (key, value) in extract_extra_destinations(&body, "Update") {
            match stream.extra_destinations.get_mut(&key) {
                Some(existing) => merge_json(existing, &value),
                None => {
                    stream.extra_destinations.insert(key, value);
                }
            }
        }

        // A successful update advances the stream version.
        stream.version_id = next_version_id(&stream.version_id);
        stream.last_update = Utc::now();
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn start_delivery_stream_encryption(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["DeliveryStreamName"]
            .as_str()
            .ok_or_else(|| missing("DeliveryStreamName"))?;

        let encryption =
            parse_encryption_input(&body["DeliveryStreamEncryptionConfigurationInput"])?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let stream = state
            .streams_mut(&req.region)
            .get_mut(name)
            .ok_or_else(|| not_found(name))?;
        stream.encryption = Some(encryption);
        stream.last_update = Utc::now();
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn stop_delivery_stream_encryption(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["DeliveryStreamName"]
            .as_str()
            .ok_or_else(|| missing("DeliveryStreamName"))?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let stream = state
            .streams_mut(&req.region)
            .get_mut(name)
            .ok_or_else(|| not_found(name))?;
        stream.encryption = Some(EncryptionConfig {
            status: "DISABLED".to_string(),
            key_type: None,
            key_arn: None,
        });
        stream.last_update = Utc::now();
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub fn deliver_records(
        &self,
        account_id: &str,
        region: &str,
        stream_name: &str,
        datas: Vec<Vec<u8>>,
    ) -> Result<(), AwsServiceError> {
        let (dest, version_id) = {
            let accounts = self.state.read();
            let state = accounts
                .get(account_id)
                .ok_or_else(|| not_found(stream_name))?;
            let stream = state
                .streams(region)
                .and_then(|s| s.get(stream_name))
                .ok_or_else(|| not_found(stream_name))?;
            (stream.destination.clone(), stream.version_id.clone())
        };
        let Some(dest) = dest else {
            return Ok(());
        };
        let Some(s3) = &self.s3 else {
            return Ok(());
        };
        let Some(bucket_name) = bucket_name_from_arn(&dest.bucket_arn) else {
            // AWS accepts PutRecord/PutRecordBatch into the delivery buffer and
            // reports delivery failures asynchronously (CloudWatch metrics /
            // S3 error-output), never as an API error, so the caller still gets
            // a 200 + RecordId. We can't deliver a malformed destination ARN, so
            // warn for observability instead of dropping it silently.
            tracing::warn!(
                stream = %stream_name,
                bucket_arn = %dest.bucket_arn,
                "firehose S3 destination has a malformed bucket ARN; records not delivered",
            );
            return Ok(());
        };
        let output = OutputConfig {
            prefix: dest.prefix.as_deref().unwrap_or_default(),
            file_extension: dest.file_extension.as_deref(),
            compression: match dest.compression_format.as_deref() {
                Some("GZIP") => Compression::Gzip,
                Some("Snappy") => Compression::Snappy,
                _ => Compression::Uncompressed,
            },
            append_delimiter: append_delimiter_enabled(dest.processing_configuration.as_ref()),
        };
        // Judge emptiness by the uncompressed payload: GZIP encodes even an
        // empty payload into nonempty bytes, while an appended delimiter makes
        // any record nonempty.
        let delimited = output.append_delimiter && !datas.is_empty();
        if !delimited && datas.iter().all(Vec::is_empty) {
            return Ok(());
        }
        let bytes = match encode_records(&datas, &output) {
            Ok(bytes) => bytes,
            Err(err) => {
                tracing::error!(stream = %stream_name, %err, "failed to encode firehose S3 delivery; records not delivered");
                return Ok(());
            }
        };
        let now = Utc::now();
        let key = object_key(stream_name, &version_id, &output, now, now, Uuid::new_v4());
        // Snapshot the durable store handle before taking the S3 state lock so we
        // never hold two locks at once.
        let store = self.s3_store.read().clone();
        let bucket_exists = s3
            .read()
            .get(account_id)
            .is_some_and(|state| state.buckets.contains_key(bucket_name));
        if !bucket_exists {
            // As above: AWS acknowledges the records (async delivery) even when
            // the destination bucket is absent. Warn for observability rather
            // than dropping silently, but don't fail the API call.
            tracing::warn!(
                stream = %stream_name,
                bucket = %bucket_name,
                "firehose S3 destination bucket does not exist; records not delivered",
            );
            return Ok(());
        }
        let write = |store: Option<&dyn S3Store>| {
            put_object_via_store_with_options(
                s3,
                store,
                DeliveryObjectRequest {
                    account: account_id,
                    bucket: bucket_name,
                    key: &key,
                    body: bytes.clone(),
                    options: DeliveryObjectOptions {
                        content_type: None,
                        content_encoding: output.compression.content_encoding().map(str::to_owned),
                    },
                },
            )
        };
        if let Err(err) = write(store.as_deref()) {
            if store.is_some() {
                // Preserve immediate delivery's session-only fallback until the
                // buffered engine can retain and retry failed batches.
                tracing::error!(stream = %stream_name, bucket = %bucket_name, %err,
                    "failed to persist firehose delivery to S3 store; attempting memory-only delivery");
                if let Err(err) = write(None) {
                    tracing::warn!(stream = %stream_name, bucket = %bucket_name, %err,
                        "firehose S3 fallback failed; records not delivered");
                }
            } else {
                tracing::warn!(stream = %stream_name, bucket = %bucket_name, %err,
                    "firehose S3 delivery failed; records not delivered");
            }
        }
        Ok(())
    }
}

fn not_found(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ResourceNotFoundException",
        format!("DeliveryStream {name} not found"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::{HeaderMap, Method};

    fn request(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "firehose".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "req-1".to_string(),
            headers: HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn service() -> FirehoseService {
        FirehoseService::default()
    }

    fn create(svc: &FirehoseService, name: &str) {
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({ "DeliveryStreamName": name }),
        ))
        .unwrap();
    }

    #[test]
    fn create_delivery_stream_persists_tags() {
        // Create-time Tags must surface via ListTagsForDeliveryStream without a
        // follow-up TagDeliveryStream (bug-hunt).
        let svc = service();
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "tagged-at-create",
                "Tags": [
                    {"Key": "team", "Value": "data"},
                    {"Key": "env", "Value": "prod"}
                ]
            }),
        ))
        .unwrap();

        let resp = svc
            .list_tags_for_delivery_stream(&request(
                "ListTagsForDeliveryStream",
                json!({ "DeliveryStreamName": "tagged-at-create" }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let tags = body["Tags"].as_array().unwrap();
        assert_eq!(tags.len(), 2);
        let mut pairs: Vec<(&str, &str)> = tags
            .iter()
            .map(|t| (t["Key"].as_str().unwrap(), t["Value"].as_str().unwrap()))
            .collect();
        pairs.sort();
        assert_eq!(pairs, vec![("env", "prod"), ("team", "data")]);
    }

    fn describe_encryption(svc: &FirehoseService, name: &str) -> Value {
        let resp = svc
            .describe_delivery_stream(&request(
                "DescribeDeliveryStream",
                json!({ "DeliveryStreamName": name }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        body["DeliveryStreamDescription"]["DeliveryStreamEncryptionConfiguration"].clone()
    }

    #[test]
    fn create_with_encryption_input_reflected_in_describe() {
        let svc = service();
        let key = "arn:aws:kms:us-east-1:123456789012:key/abcd";
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "enc-at-create",
                "DeliveryStreamEncryptionConfigurationInput": {
                    "KeyType": "CUSTOMER_MANAGED_CMK",
                    "KeyARN": key
                }
            }),
        ))
        .unwrap();
        assert_eq!(
            describe_encryption(&svc, "enc-at-create"),
            json!({"Status": "ENABLED", "KeyType": "CUSTOMER_MANAGED_CMK", "KeyARN": key})
        );

        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "enc-default",
                "DeliveryStreamEncryptionConfigurationInput": {}
            }),
        ))
        .unwrap();
        assert_eq!(
            describe_encryption(&svc, "enc-default"),
            json!({"Status": "ENABLED", "KeyType": "AWS_OWNED_CMK"})
        );

        let err = svc
            .create_delivery_stream(&request(
                "CreateDeliveryStream",
                json!({
                    "DeliveryStreamName": "enc-bad",
                    "DeliveryStreamEncryptionConfigurationInput": {"KeyType": "CUSTOMER_MANAGED_CMK"}
                }),
            ))
            .err()
            .expect("CUSTOMER_MANAGED_CMK without KeyARN is rejected");
        assert_eq!(err.code(), "InvalidArgumentException");
    }

    #[test]
    fn start_encryption_customer_managed_reflected_in_describe() {
        let svc = service();
        create(&svc, "enc-stream");
        svc.start_delivery_stream_encryption(&request(
            "StartDeliveryStreamEncryption",
            json!({
                "DeliveryStreamName": "enc-stream",
                "DeliveryStreamEncryptionConfigurationInput": {
                    "KeyType": "CUSTOMER_MANAGED_CMK",
                    "KeyARN": "arn:aws:kms:us-east-1:123456789012:key/abc"
                }
            }),
        ))
        .unwrap();

        let enc = describe_encryption(&svc, "enc-stream");
        assert_eq!(enc["Status"], "ENABLED");
        assert_eq!(enc["KeyType"], "CUSTOMER_MANAGED_CMK");
        assert_eq!(enc["KeyARN"], "arn:aws:kms:us-east-1:123456789012:key/abc");
    }

    #[test]
    fn start_encryption_defaults_to_aws_owned_cmk() {
        let svc = service();
        create(&svc, "default-enc");
        svc.start_delivery_stream_encryption(&request(
            "StartDeliveryStreamEncryption",
            json!({ "DeliveryStreamName": "default-enc" }),
        ))
        .unwrap();

        let enc = describe_encryption(&svc, "default-enc");
        assert_eq!(enc["Status"], "ENABLED");
        assert_eq!(enc["KeyType"], "AWS_OWNED_CMK");
        assert!(enc["KeyARN"].is_null());
    }

    #[test]
    fn stop_encryption_sets_disabled() {
        let svc = service();
        create(&svc, "stop-enc");
        svc.start_delivery_stream_encryption(&request(
            "StartDeliveryStreamEncryption",
            json!({ "DeliveryStreamName": "stop-enc" }),
        ))
        .unwrap();
        svc.stop_delivery_stream_encryption(&request(
            "StopDeliveryStreamEncryption",
            json!({ "DeliveryStreamName": "stop-enc" }),
        ))
        .unwrap();

        let enc = describe_encryption(&svc, "stop-enc");
        assert_eq!(enc["Status"], "DISABLED");
        assert!(enc["KeyType"].is_null());
    }

    #[test]
    fn start_encryption_unknown_stream_not_found() {
        let svc = service();
        let result = svc.start_delivery_stream_encryption(&request(
            "StartDeliveryStreamEncryption",
            json!({ "DeliveryStreamName": "ghost" }),
        ));
        let Err(err) = result else {
            panic!("expected ResourceNotFoundException");
        };
        assert!(format!("{err:?}").contains("ResourceNotFoundException"));
    }

    #[test]
    fn stop_encryption_unknown_stream_not_found() {
        let svc = service();
        let result = svc.stop_delivery_stream_encryption(&request(
            "StopDeliveryStreamEncryption",
            json!({ "DeliveryStreamName": "ghost" }),
        ));
        let Err(err) = result else {
            panic!("expected ResourceNotFoundException");
        };
        assert!(format!("{err:?}").contains("ResourceNotFoundException"));
    }

    #[test]
    fn buffering_size_within_range_ok() {
        assert!(validate_buffering(Some(64), Some(300)).is_ok());
    }

    #[test]
    fn buffering_size_too_small_rejected() {
        assert!(validate_buffering(Some(0), None).is_err());
    }

    #[test]
    fn buffering_size_too_large_rejected() {
        assert!(validate_buffering(Some(200), None).is_err());
    }

    #[test]
    fn buffering_interval_zero_ok() {
        assert!(validate_buffering(None, Some(0)).is_ok());
    }

    #[test]
    fn buffering_interval_below_60_rejected() {
        assert!(validate_buffering(None, Some(30)).is_err());
    }

    #[test]
    fn buffering_interval_above_900_rejected() {
        assert!(validate_buffering(None, Some(1200)).is_err());
    }

    #[test]
    fn create_with_processing_configuration_round_trips() {
        // ProcessingConfiguration (Lambda transform) on an ExtendedS3 destination
        // was dropped; it must round-trip through DescribeDeliveryStream
        // (bug-audit 2026-06-20, 1.18).
        let svc = service();
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "proc-stream",
                "ExtendedS3DestinationConfiguration": {
                    "RoleARN": "arn:aws:iam::123456789012:role/fh",
                    "BucketARN": "arn:aws:s3:::bucket",
                    "ProcessingConfiguration": {
                        "Enabled": true,
                        "Processors": [{ "Type": "Lambda" }]
                    }
                }
            }),
        ))
        .unwrap();

        let resp = svc
            .describe_delivery_stream(&request(
                "DescribeDeliveryStream",
                json!({ "DeliveryStreamName": "proc-stream" }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let dests = &body["DeliveryStreamDescription"]["Destinations"];
        let ext = &dests[0]["ExtendedS3DestinationDescription"];
        assert_eq!(ext["ProcessingConfiguration"]["Enabled"], true, "{body}");
        assert_eq!(
            ext["ProcessingConfiguration"]["Processors"][0]["Type"],
            "Lambda"
        );
    }

    fn describe(svc: &FirehoseService, name: &str) -> Value {
        let resp = svc
            .describe_delivery_stream(&request(
                "DescribeDeliveryStream",
                json!({ "DeliveryStreamName": name }),
            ))
            .unwrap();
        serde_json::from_slice::<Value>(resp.body.expect_bytes()).unwrap()
            ["DeliveryStreamDescription"]
            .clone()
    }

    #[test]
    fn update_destination_keeps_destination_id_stable_and_bumps_version() {
        let svc = service();
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "upd-stream",
                "ExtendedS3DestinationConfiguration": {
                    "RoleARN": "arn:aws:iam::123456789012:role/fh",
                    "BucketARN": "arn:aws:s3:::bucket",
                    "Prefix": "before/"
                }
            }),
        ))
        .unwrap();

        let before = describe(&svc, "upd-stream");
        let original_dest_id = before["Destinations"][0]["DestinationId"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(before["VersionId"], "1");

        // Partial update: only change the Prefix (no RoleARN/BucketARN).
        svc.update_destination(&request(
            "UpdateDestination",
            json!({
                "DeliveryStreamName": "upd-stream",
                "CurrentDeliveryStreamVersionId": "1",
                "DestinationId": original_dest_id,
                "ExtendedS3DestinationUpdate": { "Prefix": "after/" }
            }),
        ))
        .unwrap();

        let after = describe(&svc, "upd-stream");
        // DestinationId is stable across the update.
        assert_eq!(
            after["Destinations"][0]["DestinationId"].as_str().unwrap(),
            original_dest_id
        );
        // Partial update applied: Prefix changed, BucketARN preserved.
        assert_eq!(
            after["Destinations"][0]["ExtendedS3DestinationDescription"]["Prefix"],
            "after/"
        );
        assert_eq!(
            after["Destinations"][0]["ExtendedS3DestinationDescription"]["BucketARN"],
            "arn:aws:s3:::bucket"
        );
        // Version advanced from 1 -> 2.
        assert_eq!(after["VersionId"], "2");
    }

    #[test]
    fn update_non_s3_destination_merges_field_wise() {
        // A partial RedshiftDestinationUpdate must merge onto the stored
        // description instead of replacing it, so untouched fields survive.
        let svc = service();
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "rs-stream",
                "RedshiftDestinationConfiguration": {
                    "RoleARN": "arn:aws:iam::123456789012:role/fh",
                    "ClusterJDBCURL": "jdbc:redshift://cluster:5439/db",
                    "Username": "admin",
                    "CopyCommand": { "DataTableName": "events" },
                    "RetryOptions": { "DurationInSeconds": 3600 }
                }
            }),
        ))
        .unwrap();

        let dest_id = describe(&svc, "rs-stream")["Destinations"][0]["DestinationId"]
            .as_str()
            .unwrap()
            .to_string();

        // Update only the RetryOptions sub-field.
        svc.update_destination(&request(
            "UpdateDestination",
            json!({
                "DeliveryStreamName": "rs-stream",
                "CurrentDeliveryStreamVersionId": "1",
                "DestinationId": dest_id,
                "RedshiftDestinationUpdate": {
                    "RetryOptions": { "DurationInSeconds": 7200 }
                }
            }),
        ))
        .unwrap();

        let after = describe(&svc, "rs-stream");
        let rs = &after["Destinations"][0]["RedshiftDestinationDescription"];
        // Changed field applied.
        assert_eq!(rs["RetryOptions"]["DurationInSeconds"], 7200, "{after}");
        // Untouched fields preserved (not dropped by a whole-object replace).
        assert_eq!(rs["ClusterJDBCURL"], "jdbc:redshift://cluster:5439/db");
        assert_eq!(rs["Username"], "admin");
        assert_eq!(rs["CopyCommand"]["DataTableName"], "events");
        assert_eq!(rs["RoleARN"], "arn:aws:iam::123456789012:role/fh");
    }

    #[test]
    fn extended_s3_dynamic_partition_encryption_backup_round_trip() {
        // DynamicPartitioningConfiguration, EncryptionConfiguration and the
        // nested S3 backup destination were dropped entirely; all three must
        // round-trip through DescribeDeliveryStream.
        let svc = service();
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "ext-stream",
                "ExtendedS3DestinationConfiguration": {
                    "RoleARN": "arn:aws:iam::123456789012:role/fh",
                    "BucketARN": "arn:aws:s3:::bucket",
                    "DynamicPartitioningConfiguration": {
                        "Enabled": true,
                        "RetryOptions": { "DurationInSeconds": 300 }
                    },
                    "EncryptionConfiguration": {
                        "KMSEncryptionConfig": {
                            "AWSKMSKeyARN": "arn:aws:kms:us-east-1:123456789012:key/abc"
                        }
                    },
                    "S3BackupMode": "Enabled",
                    "S3BackupConfiguration": {
                        "RoleARN": "arn:aws:iam::123456789012:role/backup",
                        "BucketARN": "arn:aws:s3:::backup-bucket",
                        "Prefix": "backup/"
                    }
                }
            }),
        ))
        .unwrap();

        let desc = describe(&svc, "ext-stream");
        let ext = &desc["Destinations"][0]["ExtendedS3DestinationDescription"];
        assert_eq!(
            ext["DynamicPartitioningConfiguration"]["Enabled"], true,
            "{desc}"
        );
        assert_eq!(
            ext["EncryptionConfiguration"]["KMSEncryptionConfig"]["AWSKMSKeyARN"],
            "arn:aws:kms:us-east-1:123456789012:key/abc"
        );
        assert_eq!(ext["S3BackupMode"], "Enabled");
        assert_eq!(
            ext["S3BackupDescription"]["BucketARN"],
            "arn:aws:s3:::backup-bucket"
        );
        assert_eq!(ext["S3BackupDescription"]["Prefix"], "backup/");
    }

    fn create_ver_stream(svc: &FirehoseService) -> String {
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "ver-stream",
                "ExtendedS3DestinationConfiguration": {
                    "RoleARN": "arn:aws:iam::123456789012:role/fh",
                    "BucketARN": "arn:aws:s3:::bucket"
                }
            }),
        ))
        .unwrap();
        describe(svc, "ver-stream")["Destinations"][0]["DestinationId"]
            .as_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn update_destination_rejects_stale_version() {
        let svc = service();
        let dest_id = create_ver_stream(&svc);

        // Wrong CurrentDeliveryStreamVersionId -> ConcurrentModificationException.
        let err = match svc.update_destination(&request(
            "UpdateDestination",
            json!({
                "DeliveryStreamName": "ver-stream",
                "CurrentDeliveryStreamVersionId": "99",
                "DestinationId": dest_id,
                "ExtendedS3DestinationUpdate": { "Prefix": "x/" }
            }),
        )) {
            Err(e) => e,
            Ok(_) => panic!("stale version must be rejected"),
        };
        assert_eq!(err.code(), "ConcurrentModificationException");
    }

    #[test]
    fn update_destination_requires_current_version_and_destination_id() {
        let svc = service();
        let dest_id = create_ver_stream(&svc);

        // Missing CurrentDeliveryStreamVersionId -> rejected.
        let err = match svc.update_destination(&request(
            "UpdateDestination",
            json!({
                "DeliveryStreamName": "ver-stream",
                "DestinationId": dest_id,
                "ExtendedS3DestinationUpdate": { "Prefix": "x/" }
            }),
        )) {
            Err(e) => e,
            Ok(_) => panic!("missing CurrentDeliveryStreamVersionId must be rejected"),
        };
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);

        // Missing DestinationId -> rejected.
        let err = match svc.update_destination(&request(
            "UpdateDestination",
            json!({
                "DeliveryStreamName": "ver-stream",
                "CurrentDeliveryStreamVersionId": "1",
                "ExtendedS3DestinationUpdate": { "Prefix": "x/" }
            }),
        )) {
            Err(e) => e,
            Ok(_) => panic!("missing DestinationId must be rejected"),
        };
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn update_destination_rejects_mismatched_destination_id() {
        let svc = service();
        create_ver_stream(&svc);

        let err = match svc.update_destination(&request(
            "UpdateDestination",
            json!({
                "DeliveryStreamName": "ver-stream",
                "CurrentDeliveryStreamVersionId": "1",
                "DestinationId": "destinationId-wrong",
                "ExtendedS3DestinationUpdate": { "Prefix": "x/" }
            }),
        )) {
            Err(e) => e,
            Ok(_) => panic!("mismatched DestinationId must be rejected"),
        };
        assert_eq!(err.code(), "InvalidArgumentException");
    }

    #[test]
    fn list_tags_for_delivery_stream_paginates() {
        // Regression: ListTagsForDeliveryStream honors Limit +
        // ExclusiveStartTagKey and sets HasMoreTags when a page is truncated,
        // instead of returning every tag with HasMoreTags hardcoded false.
        let svc = service();
        create(&svc, "tagged");
        svc.tag_delivery_stream(&request(
            "TagDeliveryStream",
            json!({
                "DeliveryStreamName": "tagged",
                "Tags": [
                    {"Key": "a", "Value": "1"},
                    {"Key": "b", "Value": "2"},
                    {"Key": "c", "Value": "3"}
                ]
            }),
        ))
        .unwrap();

        // Page 1: Limit 2 -> first two keys, more remain.
        let resp = svc
            .list_tags_for_delivery_stream(&request(
                "ListTagsForDeliveryStream",
                json!({ "DeliveryStreamName": "tagged", "Limit": 2 }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let tags = body["Tags"].as_array().unwrap();
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0]["Key"], "a");
        assert_eq!(tags[1]["Key"], "b");
        assert_eq!(body["HasMoreTags"], true);

        // Page 2: resume after "b".
        let resp = svc
            .list_tags_for_delivery_stream(&request(
                "ListTagsForDeliveryStream",
                json!({
                    "DeliveryStreamName": "tagged",
                    "Limit": 2,
                    "ExclusiveStartTagKey": "b"
                }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let tags = body["Tags"].as_array().unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0]["Key"], "c");
        assert_eq!(body["HasMoreTags"], false);
    }

    const S3_ACCOUNT: &str = "123456789012";

    /// Build an S3 state seeded with `bucket`, plus a matching on-disk
    /// [`DiskS3Store`] (with the bucket's meta so `load()` surfaces it), and a
    /// FirehoseService wired to both.
    fn firehose_with_disk_s3(
        bucket: &str,
    ) -> (
        FirehoseService,
        std::sync::Arc<fakecloud_persistence::s3::DiskS3Store>,
        tempfile::TempDir,
    ) {
        use fakecloud_core::multi_account::MultiAccountState;
        use fakecloud_persistence::{cache::BodyCache, s3::DiskS3Store, BucketMeta, S3Store};
        use fakecloud_s3::{S3Bucket, S3State};

        let tmp = tempfile::TempDir::new().unwrap();
        let cache = std::sync::Arc::new(BodyCache::new(1024 * 1024));
        let disk = std::sync::Arc::new(DiskS3Store::new(tmp.path().join("s3"), cache));
        disk.put_bucket_meta(
            bucket,
            &BucketMeta {
                name: bucket.to_string(),
                region: "us-east-1".to_string(),
                ..Default::default()
            },
        )
        .unwrap();

        let mut mas: MultiAccountState<S3State> =
            MultiAccountState::new(S3_ACCOUNT, "us-east-1", "http://localhost");
        mas.get_or_create(S3_ACCOUNT).buckets.insert(
            bucket.to_string(),
            S3Bucket::new(bucket, "us-east-1", "owner-id"),
        );
        let s3_state = std::sync::Arc::new(RwLock::new(mas));

        let svc = FirehoseService::new(Arc::new(RwLock::new(FirehoseAccounts::new())))
            .with_s3(s3_state)
            .with_s3_store(disk.clone() as std::sync::Arc<dyn S3Store>);
        (svc, disk, tmp)
    }

    fn create_s3_stream(svc: &FirehoseService, name: &str, bucket: &str) {
        create_s3_stream_with_compression(svc, name, bucket, None);
    }

    fn create_s3_stream_with_compression(
        svc: &FirehoseService,
        name: &str,
        bucket: &str,
        compression: Option<&str>,
    ) {
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": name,
                "ExtendedS3DestinationConfiguration": {
                    "RoleARN": "arn:aws:iam::123456789012:role/fh",
                    "BucketARN": format!("arn:aws:s3:::{bucket}"),
                    "CompressionFormat": compression,
                }
            }),
        ))
        .unwrap();
    }

    #[test]
    fn snappy_delivery_uses_the_modeled_enum_and_xerial_metadata() {
        use crate::test_xerial_decoder::{decode, multiblock_input};

        let (svc, disk, _tmp) = firehose_with_disk_s3("fh-snappy");
        create_s3_stream_with_compression(&svc, "snappy", "fh-snappy", Some("Snappy"));
        let input = multiblock_input();
        svc.deliver_records(
            S3_ACCOUNT,
            "us-east-1",
            "snappy",
            vec![b"abc".to_vec(), b"def\n".to_vec(), input.clone()],
        )
        .unwrap();
        let s3 = svc.s3.as_ref().unwrap().read();
        let bucket = &s3.get(S3_ACCOUNT).unwrap().buckets["fh-snappy"];
        assert_eq!(bucket.objects.len(), 1);
        let object = bucket.objects.values().next().unwrap();
        assert!(object.key.ends_with(".snappy"));
        assert_eq!(object.content_encoding.as_deref(), Some("snappy-java"));
        assert_eq!(object.content_type, "application/octet-stream");
        let raw = disk.open_object_body(&object.body).unwrap();
        assert_eq!(object.size, raw.len() as u64);
        let blocks = decode(&raw).unwrap();
        assert_eq!(
            blocks.iter().map(Vec::len).collect::<Vec<_>>(),
            [32768, 32768, 4471]
        );
        let decoded = blocks.concat();
        assert_eq!(&decoded[..7], b"abcdef\n");
        assert_eq!(&decoded[7..], input);
    }

    #[test]
    fn gzip_delivery_encodes_bytes_and_persists_metadata() {
        use std::io::Read;

        let (svc, disk, _tmp) = firehose_with_disk_s3("fh-gzip");
        create_s3_stream_with_compression(&svc, "gzip", "fh-gzip", Some("GZIP"));
        svc.deliver_records(
            S3_ACCOUNT,
            "us-east-1",
            "gzip",
            vec![b"abc".to_vec(), b"def\n".to_vec(), vec![0, 255]],
        )
        .unwrap();
        let s3 = svc.s3.as_ref().unwrap().read();
        let bucket = &s3.get(S3_ACCOUNT).unwrap().buckets["fh-gzip"];
        assert_eq!(bucket.objects.len(), 1);
        let object = bucket.objects.values().next().unwrap();
        assert!(object.key.ends_with(".gz"));
        assert_eq!(object.content_type, "application/octet-stream");
        assert_eq!(object.content_encoding.as_deref(), Some("gzip"));
        let raw = disk.open_object_body(&object.body).unwrap();
        assert!(raw.starts_with(&[0x1f, 0x8b, 8]));
        assert_eq!(object.size, raw.len() as u64);
        let mut decoded = Vec::new();
        flate2::read::GzDecoder::new(raw.as_ref())
            .read_to_end(&mut decoded)
            .unwrap();
        assert_eq!(decoded, b"abcdef\n\x00\xff");
        let key = object.key.clone();
        drop(s3);

        let loaded = disk.load().unwrap();
        let persisted = &loaded.buckets["fh-gzip"].objects[&key];
        assert_eq!(persisted.meta.content_type, "application/octet-stream");
        assert_eq!(persisted.meta.content_encoding.as_deref(), Some("gzip"));
        assert_eq!(disk.open_object_body(&persisted.body).unwrap(), raw);
    }

    #[test]
    fn gzip_store_failure_keeps_compressed_body_in_memory() {
        use fakecloud_persistence::{cache::BodyCache, s3::DiskS3Store, BodyRef};
        use std::io::Read;

        for fail_staging in [true, false] {
            let (svc, _disk, tmp) = firehose_with_disk_s3("fh-fallback");
            create_s3_stream_with_compression(&svc, "fallback", "fh-fallback", Some("GZIP"));
            if fail_staging {
                let blocked = tmp.path().join("not-a-directory");
                std::fs::write(&blocked, b"blocks persistent writes").unwrap();
                svc.set_s3_store(Arc::new(DiskS3Store::new(
                    blocked,
                    Arc::new(BodyCache::new(1024)),
                )));
            } else {
                // Staging can succeed, but the final object directory cannot be created.
                std::fs::write(
                    tmp.path().join("s3/buckets/fh-fallback/objects"),
                    b"blocks commit",
                )
                .unwrap();
            }
            let response = svc.put_record(&request("PutRecord", json!({
            "DeliveryStreamName": "fallback",
            "Record": {"Data": base64::engine::general_purpose::STANDARD.encode(b"\x00\xff")}
        }))).unwrap();
            let response: Value = serde_json::from_slice(response.body.expect_bytes()).unwrap();
            assert!(response["RecordId"].as_str().is_some());

            let s3 = svc.s3.as_ref().unwrap().read();
            let bucket = &s3.get(S3_ACCOUNT).unwrap().buckets["fh-fallback"];
            assert_eq!(
                bucket.objects.len(),
                1,
                "failed persistence must retain the session fallback"
            );
            let object = bucket.objects.values().next().unwrap();
            assert_eq!(object.content_encoding.as_deref(), Some("gzip"));
            let BodyRef::Memory(raw) = &object.body else {
                panic!("failed write cannot be disk-backed")
            };
            assert_eq!(object.size, raw.len() as u64);
            let mut decoded = Vec::new();
            flate2::read::GzDecoder::new(raw.as_ref())
                .read_to_end(&mut decoded)
                .unwrap();
            assert_eq!(decoded, b"\x00\xff");
        }
    }

    #[test]
    fn empty_gzip_delivery_does_not_create_an_object() {
        let (svc, _disk, _tmp) = firehose_with_disk_s3("fh-empty-gzip");
        create_s3_stream_with_compression(&svc, "empty-gzip", "fh-empty-gzip", Some("GZIP"));
        svc.deliver_records(S3_ACCOUNT, "us-east-1", "empty-gzip", vec![vec![], vec![]])
            .unwrap();
        let s3 = svc.s3.as_ref().unwrap().read();
        assert!(s3.get(S3_ACCOUNT).unwrap().buckets["fh-empty-gzip"]
            .objects
            .is_empty());
    }

    /// `AppendDelimiterToRecord` runs before compression: every record of an
    /// API or internal delivery, including an empty one (which alone would not
    /// produce a GZIP object), is followed by `\n` inside the GZIP stream.
    #[test]
    fn gzip_compresses_records_after_append_delimiter() {
        use fakecloud_core::delivery::FirehoseDelivery;
        use std::io::Read;

        let (svc, disk, _tmp) = firehose_with_disk_s3("fh-gzip-delim");
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({
                "DeliveryStreamName": "gzip-delim",
                "ExtendedS3DestinationConfiguration": {
                    "RoleARN": "arn:aws:iam::123456789012:role/fh",
                    "BucketARN": "arn:aws:s3:::fh-gzip-delim",
                    "CompressionFormat": "GZIP",
                    "ProcessingConfiguration": {
                        "Enabled": true,
                        "Processors": [{"Type": "AppendDelimiterToRecord"}]
                    }
                }
            }),
        ))
        .unwrap();
        svc.deliver_records(
            S3_ACCOUNT,
            "us-east-1",
            "gzip-delim",
            vec![b"a".to_vec(), b"b\n".to_vec(), vec![0, 255]],
        )
        .unwrap();
        svc.deliver_records(S3_ACCOUNT, "us-east-1", "gzip-delim", vec![vec![]])
            .unwrap();
        let hook =
            crate::FirehoseDeliveryImpl::new(svc.shared_state(), svc.s3.as_ref().unwrap().clone());
        hook.set_s3_store(disk.clone());
        hook.put_record(
            "arn:aws:firehose:us-east-1:123456789012:deliverystream/gzip-delim",
            b"{\"internal\":true}",
        );

        let s3 = svc.s3.as_ref().unwrap().read();
        let mut decoded: Vec<Vec<u8>> = s3.get(S3_ACCOUNT).unwrap().buckets["fh-gzip-delim"]
            .objects
            .values()
            .map(|object| {
                assert!(object.key.ends_with(".gz"), "{}", object.key);
                assert_eq!(object.content_encoding.as_deref(), Some("gzip"));
                let raw = disk.open_object_body(&object.body).unwrap();
                assert_eq!(object.size, raw.len() as u64);
                let mut plain = Vec::new();
                flate2::read::GzDecoder::new(raw.as_ref())
                    .read_to_end(&mut plain)
                    .unwrap();
                plain
            })
            .collect();
        decoded.sort();
        assert_eq!(
            decoded,
            vec![
                b"\n".to_vec(),
                b"a\nb\n\n\x00\xff\n".to_vec(),
                b"{\"internal\":true}\n".to_vec(),
            ]
        );
    }

    #[test]
    fn delivery_preserves_binary_bytes_and_producer_delimiters() {
        let (svc, disk, _tmp) = firehose_with_disk_s3("fh-bytes");
        create_s3_stream(&svc, "verbatim", "fh-bytes");
        svc.deliver_records(
            S3_ACCOUNT,
            "us-east-1",
            "verbatim",
            vec![b"abc".to_vec(), b"def\n".to_vec(), vec![0, 255]],
        )
        .unwrap();

        let s3 = svc.s3.as_ref().unwrap().read();
        let bucket = &s3.get(S3_ACCOUNT).unwrap().buckets["fh-bytes"];
        assert_eq!(bucket.objects.len(), 1);
        let object = bucket.objects.values().next().unwrap();
        assert_eq!(
            disk.open_object_body(&object.body).unwrap().as_ref(),
            b"abcdef\n\x00\xff"
        );
        assert_eq!(object.size, 9);
        // S3 stores the bare MD5 hex (GetObject/HeadObject/List add the quotes),
        // as for any single-part PutObject.
        assert_eq!(object.etag, "7d2ae660248a023760f924dd991e2533");
    }

    #[test]
    fn delivery_names_use_stream_version_utc_and_custom_extension() {
        for (prefix, extension) in [
            (None, None),
            (Some("events/"), Some(".custom.log")),
            (Some("events"), Some("")),
            (Some("../logical/"), Some(".log")),
        ] {
            let (svc, disk, _tmp) = firehose_with_disk_s3("fh-names");
            create_s3_stream(&svc, "named", "fh-names");
            let destination_id = svc
                .state
                .read()
                .get(S3_ACCOUNT)
                .unwrap()
                .streams("us-east-1")
                .unwrap()["named"]
                .destination
                .as_ref()
                .unwrap()
                .destination_id
                .clone();
            let mut update = json!({});
            if let Some(prefix) = prefix {
                update["Prefix"] = json!(prefix);
            }
            if let Some(extension) = extension {
                update["FileExtension"] = json!(extension);
            }
            svc.update_destination(&request(
                "UpdateDestination",
                json!({
                    "DeliveryStreamName": "named",
                    "CurrentDeliveryStreamVersionId": "1",
                    "DestinationId": destination_id,
                    "ExtendedS3DestinationUpdate": update
                }),
            ))
            .unwrap();
            let before = Utc::now();
            svc.deliver_records(S3_ACCOUNT, "us-east-1", "named", vec![b"x".to_vec()])
                .unwrap();
            let after = Utc::now();

            let s3 = svc.s3.as_ref().unwrap().read();
            let bucket = &s3.get(S3_ACCOUNT).unwrap().buckets["fh-names"];
            let object = bucket.objects.values().next().unwrap();
            // S3's LastModified is assigned after staging, so it need not share
            // the key's delivery second when disk I/O spans a time boundary.
            let name = object.key.strip_prefix(prefix.unwrap_or_default()).unwrap();
            let (hierarchy, suffix) = name.rsplit_once("/named-2-").unwrap();
            let (timestamp, uuid_and_extension) = suffix.split_at(19);
            let time = chrono::NaiveDateTime::parse_from_str(timestamp, "%Y-%m-%d-%H-%M-%S")
                .unwrap()
                .and_utc();
            assert!((before.timestamp()..=after.timestamp()).contains(&time.timestamp()));
            assert_eq!(hierarchy, time.format("%Y/%m/%d/%H").to_string());
            let uuid_and_extension = uuid_and_extension.strip_prefix('-').unwrap();
            let uuid = uuid_and_extension
                .strip_suffix(extension.unwrap_or_default())
                .unwrap();
            assert_eq!(uuid.len(), 36);
            assert_eq!(Uuid::parse_str(uuid).unwrap().to_string(), uuid);
            assert_eq!(disk.open_object_body(&object.body).unwrap().as_ref(), b"x");
        }
    }

    #[test]
    fn internal_delivery_preserves_exact_bytes() {
        use fakecloud_core::delivery::FirehoseDelivery;

        let (svc, disk, _tmp) = firehose_with_disk_s3("fh-hook");
        create_s3_stream(&svc, "hook", "fh-hook");
        let hook =
            crate::FirehoseDeliveryImpl::new(svc.shared_state(), svc.s3.as_ref().unwrap().clone());
        hook.set_s3_store(disk.clone());
        hook.put_record(
            "arn:aws:firehose:us-east-1:123456789012:deliverystream/hook",
            b"\x00\xff",
        );

        let s3 = svc.s3.as_ref().unwrap().read();
        let bucket = &s3.get(S3_ACCOUNT).unwrap().buckets["fh-hook"];
        assert_eq!(bucket.objects.len(), 1);
        assert_eq!(
            disk.open_object_body(&bucket.objects.values().next().unwrap().body)
                .unwrap()
                .as_ref(),
            b"\x00\xff"
        );
    }

    #[test]
    fn empty_delivery_does_not_create_an_object() {
        let (svc, _disk, _tmp) = firehose_with_disk_s3("fh-empty");
        create_s3_stream(&svc, "empty", "fh-empty");
        svc.deliver_records(S3_ACCOUNT, "us-east-1", "empty", vec![vec![], vec![]])
            .unwrap();
        let s3 = svc.s3.as_ref().unwrap().read();
        assert!(s3.get(S3_ACCOUNT).unwrap().buckets["fh-empty"]
            .objects
            .is_empty());
    }

    fn s3_destination_with(extra: Value) -> Value {
        let mut value = json!({
            "RoleARN": "arn:aws:iam::123456789012:role/fh",
            "BucketARN": "arn:aws:s3:::bucket",
        });
        value
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        value
    }

    #[test]
    fn file_extension_validation_boundaries() {
        let longest = format!(".{}", "a".repeat(127));
        for valid in ["", ".log", ".custom.log", ".a0!-_.*'()", longest.as_str()] {
            let dest = parse_s3_destination(&s3_destination_with(json!({"FileExtension": valid})))
                .unwrap()
                .unwrap();
            assert_eq!(dest.file_extension.as_deref(), Some(valid));
        }
        let too_long = format!(".{}", "a".repeat(128));
        for invalid in [
            json!("log"),
            json!("."),
            json!(".UPPER"),
            json!(".é"),
            json!(".a/b"),
            json!(".a b"),
            json!(too_long),
            json!(1),
            json!(true),
        ] {
            let err = parse_s3_destination(&s3_destination_with(json!({"FileExtension": invalid})))
                .err()
                .unwrap_or_else(|| panic!("FileExtension {invalid} must be rejected"));
            assert_eq!(err.code(), "InvalidArgumentException");
            assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        }
    }

    #[test]
    fn update_destination_rejects_invalid_file_extension_without_changes() {
        let svc = service();
        let dest_id = create_ver_stream(&svc);
        for invalid in [json!(".UPPER"), json!("log"), json!(7)] {
            let err = svc
                .update_destination(&request(
                    "UpdateDestination",
                    json!({
                        "DeliveryStreamName": "ver-stream",
                        "CurrentDeliveryStreamVersionId": "1",
                        "DestinationId": dest_id,
                        "ExtendedS3DestinationUpdate": {"Prefix": "changed/", "FileExtension": invalid}
                    }),
                ))
                .err()
                .unwrap_or_else(|| panic!("FileExtension {invalid} must be rejected"));
            assert_eq!(err.code(), "InvalidArgumentException");
        }
        let after = describe(&svc, "ver-stream");
        assert_eq!(after["VersionId"], "1");
        let ext = &after["Destinations"][0]["ExtendedS3DestinationDescription"];
        assert!(ext.get("Prefix").is_none(), "{after}");
        assert!(ext.get("FileExtension").is_none(), "{after}");
    }

    fn create_stream_with_processing(
        svc: &FirehoseService,
        name: &str,
        bucket: &str,
        processing: Value,
    ) {
        let mut destination = json!({
            "RoleARN": "arn:aws:iam::123456789012:role/fh",
            "BucketARN": format!("arn:aws:s3:::{bucket}"),
        });
        if !processing.is_null() {
            destination["ProcessingConfiguration"] = processing;
        }
        svc.create_delivery_stream(&request(
            "CreateDeliveryStream",
            json!({"DeliveryStreamName": name, "ExtendedS3DestinationConfiguration": destination}),
        ))
        .unwrap();
    }

    fn delivered_bodies(
        svc: &FirehoseService,
        disk: &fakecloud_persistence::s3::DiskS3Store,
        bucket: &str,
    ) -> Vec<Vec<u8>> {
        let s3 = svc.s3.as_ref().unwrap().read();
        let mut bodies: Vec<Vec<u8>> = s3.get(S3_ACCOUNT).unwrap().buckets[bucket]
            .objects
            .values()
            .map(|object| disk.open_object_body(&object.body).unwrap().to_vec())
            .collect();
        bodies.sort();
        bodies
    }

    /// AWS: "If you want to add a new line delimiter between records in objects
    /// that are delivered to Amazon S3, choose AppendDelimiterToRecord as a
    /// processor type." Applied per record (after each one), for API and
    /// internal (Logs/SES) deliveries alike; other processors are not run.
    #[test]
    fn append_delimiter_processor_terminates_every_record() {
        use fakecloud_core::delivery::FirehoseDelivery;

        let (svc, disk, _tmp) = firehose_with_disk_s3("fh-delim");
        create_stream_with_processing(
            &svc,
            "delim",
            "fh-delim",
            json!({"Enabled": true, "Processors": [
                {"Type": "Lambda", "Parameters": [{"ParameterName": "LambdaArn", "ParameterValue": "arn:aws:lambda:us-east-1:123456789012:function:f"}]},
                {"Type": "AppendDelimiterToRecord", "Parameters": [{"ParameterName": "Delimiter", "ParameterValue": "\\n"}]}
            ]}),
        );
        svc.deliver_records(
            S3_ACCOUNT,
            "us-east-1",
            "delim",
            vec![b"a".to_vec(), b"b\n".to_vec(), vec![0, 255]],
        )
        .unwrap();
        svc.deliver_records(S3_ACCOUNT, "us-east-1", "delim", vec![vec![]])
            .unwrap();
        let hook =
            crate::FirehoseDeliveryImpl::new(svc.shared_state(), svc.s3.as_ref().unwrap().clone());
        hook.set_s3_store(disk.clone());
        hook.put_record(
            "arn:aws:firehose:us-east-1:123456789012:deliverystream/delim",
            b"{\"internal\":true}",
        );

        assert_eq!(
            delivered_bodies(&svc, &disk, "fh-delim"),
            vec![
                b"\n".to_vec(),
                b"a\nb\n\n\x00\xff\n".to_vec(),
                b"{\"internal\":true}\n".to_vec(),
            ]
        );
    }

    #[test]
    fn append_delimiter_requires_an_enabled_processing_configuration() {
        for processing in [
            Value::Null,
            json!({"Enabled": false, "Processors": [{"Type": "AppendDelimiterToRecord"}]}),
            json!({"Processors": [{"Type": "AppendDelimiterToRecord"}]}),
            json!({"Enabled": true, "Processors": [{"Type": "Lambda"}]}),
            json!({"Enabled": true}),
        ] {
            let (svc, disk, _tmp) = firehose_with_disk_s3("fh-raw");
            create_stream_with_processing(&svc, "raw", "fh-raw", processing.clone());
            svc.deliver_records(
                S3_ACCOUNT,
                "us-east-1",
                "raw",
                vec![b"a".to_vec(), b"b".to_vec()],
            )
            .unwrap();
            assert_eq!(
                delivered_bodies(&svc, &disk, "fh-raw"),
                vec![b"ab".to_vec()],
                "{processing}"
            );
        }
    }

    #[test]
    fn put_record_writes_through_to_durable_s3_store() {
        use fakecloud_persistence::{BodyRef, S3Store};

        let (svc, disk, _tmp) = firehose_with_disk_s3("fh-bucket");
        create_s3_stream(&svc, "logs-to-s3", "fh-bucket");

        let payload = b"event-line".to_vec();
        let resp = svc
            .put_record(&request(
                "PutRecord",
                json!({
                    "DeliveryStreamName": "logs-to-s3",
                    "Record": {
                        "Data": base64::engine::general_purpose::STANDARD.encode(&payload)
                    }
                }),
            ))
            .expect("PutRecord should succeed when the destination bucket exists");
        assert!(resp
            .body
            .expect_bytes()
            .windows(8)
            .any(|w| w == b"RecordId"));

        // The delivered object's in-memory body must be the store's canonical
        // disk-backed ref (proof the write went through the store, not just RAM),
        // and the backing file must exist on disk.
        let s3 = svc.s3.as_ref().unwrap().read();
        let acct = s3.get(S3_ACCOUNT).unwrap();
        let bucket = acct.buckets.get("fh-bucket").unwrap();
        assert_eq!(bucket.objects.len(), 1, "exactly one delivered object");
        let obj = bucket.objects.values().next().unwrap();
        match &obj.body {
            BodyRef::Disk { path, .. } => {
                assert!(path.exists(), "delivered object bytes must be on disk");
            }
            BodyRef::Memory(_) => panic!("delivery was not written through to the durable store"),
        }
        drop(s3);

        // Reloading the store from disk (as boot does) must surface the object.
        let reloaded = disk.load().unwrap();
        let rbucket = reloaded
            .buckets
            .get("fh-bucket")
            .expect("bucket survives reload");
        assert_eq!(
            rbucket.objects.len(),
            1,
            "delivered object survives a store reload"
        );
    }

    #[test]
    fn put_record_to_missing_bucket_is_accepted_but_delivers_nothing() {
        // AWS accepts PutRecord into the delivery buffer and reports delivery
        // failures asynchronously (never as an API error), so a stream whose
        // destination bucket is absent still returns 200 + RecordId. The record
        // simply lands nowhere (a warn is logged for observability). This is the
        // behavior the conformance baseline asserts; the emulator must match it
        // rather than fabricate a synchronous error.
        let (svc, _disk, _tmp) = firehose_with_disk_s3("real-bucket");
        create_s3_stream(&svc, "broken-stream", "ghost-bucket");

        let resp = svc
            .put_record(&request(
                "PutRecord",
                json!({
                    "DeliveryStreamName": "broken-stream",
                    "Record": {
                        "Data": base64::engine::general_purpose::STANDARD.encode(b"x")
                    }
                }),
            ))
            .expect("PutRecord to a missing destination bucket must still be accepted (AWS async delivery)");
        assert!(resp
            .body
            .expect_bytes()
            .windows(8)
            .any(|w| w == b"RecordId"));

        // The record could not be delivered, so the existing (different) bucket
        // stays empty — nothing was written anywhere.
        let s3 = svc.s3.as_ref().unwrap().read();
        let real = s3
            .get(S3_ACCOUNT)
            .unwrap()
            .buckets
            .get("real-bucket")
            .unwrap();
        assert!(real.objects.is_empty(), "no object should be written");
    }

    #[test]
    fn china_region_delivery_stream_uses_the_aws_cn_partition() {
        let svc = service();
        let in_china = |action: &str, body: Value| AwsRequest {
            region: "cn-north-1".to_string(),
            ..request(action, body)
        };
        svc.create_delivery_stream(&in_china(
            "CreateDeliveryStream",
            json!({ "DeliveryStreamName": "cn-fh" }),
        ))
        .unwrap();
        let resp = svc
            .describe_delivery_stream(&in_china(
                "DescribeDeliveryStream",
                json!({ "DeliveryStreamName": "cn-fh" }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(
            body["DeliveryStreamDescription"]["DeliveryStreamARN"],
            "arn:aws-cn:firehose:cn-north-1:123456789012:deliverystream/cn-fh"
        );
        assert_eq!(
            bucket_name_from_arn("arn:aws-cn:s3:::cn-bucket"),
            Some("cn-bucket")
        );
        assert_eq!(bucket_name_from_arn("arn:aws:s3:::bucket"), Some("bucket"));
        assert_eq!(bucket_name_from_arn("arn:aws-cn:sqs:cn-north-1:1:q"), None);
    }
}
