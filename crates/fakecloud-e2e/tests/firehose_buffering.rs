use std::{io::Read, time::Duration};

use aws_sdk_firehose::{
    primitives::Blob,
    types::{
        BufferingHints, CompressionFormat, DataFormatConversionConfiguration,
        DynamicPartitioningConfiguration, ExtendedS3DestinationConfiguration,
        ExtendedS3DestinationUpdate, ProcessingConfiguration, Record,
    },
};
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use base64::Engine;
use fakecloud_testkit::TestServer;

#[path = "../../fakecloud-firehose/tests/fixtures/xerial_decoder.rs"]
mod xerial;

fn hints(size: i32, interval: i32) -> BufferingHints {
    BufferingHints::builder()
        .size_in_mbs(size)
        .interval_in_seconds(interval)
        .build()
}

fn destination(bucket: &str, interval: i32) -> ExtendedS3DestinationConfiguration {
    destination_builder(bucket, interval).build().unwrap()
}

fn destination_builder(
    bucket: &str,
    interval: i32,
) -> aws_sdk_firehose::types::builders::ExtendedS3DestinationConfigurationBuilder {
    ExtendedS3DestinationConfiguration::builder()
        .role_arn("arn:aws:iam::123456789012:role/firehose")
        .bucket_arn(format!("arn:aws:s3:::{bucket}"))
        .buffering_hints(hints(1, interval))
}

fn record(data: impl Into<Vec<u8>>) -> Record {
    let data = data.into();
    assert!(data.len() <= 1_024_000, "fixture exceeds AWS record limit");
    Record::builder().data(Blob::new(data)).build().unwrap()
}

async fn create(
    fh: &aws_sdk_firehose::Client,
    stream: &str,
    config: ExtendedS3DestinationConfiguration,
) {
    fh.create_delivery_stream()
        .delivery_stream_name(stream)
        .extended_s3_destination_configuration(config)
        .send()
        .await
        .unwrap();
}

async fn put(fh: &aws_sdk_firehose::Client, stream: &str, data: impl Into<Vec<u8>>) {
    let ack = fh
        .put_record()
        .delivery_stream_name(stream)
        .record(record(data))
        .send()
        .await
        .unwrap();
    assert!(!ack.record_id().is_empty());
}

async fn keys(s3: &aws_sdk_s3::Client, bucket: &str) -> Vec<String> {
    s3.list_objects_v2()
        .bucket(bucket)
        .send()
        .await
        .unwrap()
        .contents()
        .iter()
        .map(|object| object.key().unwrap().to_owned())
        .collect()
}

// Bound the entire poll, including individual SDK requests. Exact timer and
// retry-attempt contracts belong to the deterministic engine unit tests.
async fn wait_keys(s3: &aws_sdk_s3::Client, bucket: &str, count: usize) -> Vec<String> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let keys = keys(s3, bucket).await;
            assert!(keys.len() <= count, "unexpected extra objects: {keys:?}");
            if keys.len() == count {
                return keys;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{bucket}: expected {count} objects within 15s"))
}

fn gunzip(raw: &[u8]) -> Vec<u8> {
    assert!(raw.starts_with(&[0x1f, 0x8b, 8]));
    let mut decoded = Vec::new();
    flate2::read::GzDecoder::new(raw)
        .read_to_end(&mut decoded)
        .unwrap();
    decoded
}

async fn object(s3: &aws_sdk_s3::Client, bucket: &str, key: &str, codec: &str) -> Vec<u8> {
    let object = s3
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(object.content_type(), Some("application/octet-stream"));
    assert_eq!(
        object.content_encoding(),
        match codec {
            "GZIP" => Some("gzip"),
            "Snappy" => Some("snappy-java"),
            _ => None,
        }
    );
    let length = object.content_length().unwrap();
    let raw = object.body.collect().await.unwrap().into_bytes();
    assert_eq!(length, raw.len() as i64);
    match codec {
        "GZIP" => gunzip(&raw),
        "Snappy" => xerial::decode(&raw).unwrap().concat(),
        _ => raw.to_vec(),
    }
}

async fn update(fh: &aws_sdk_firehose::Client, stream: &str, config: ExtendedS3DestinationUpdate) {
    let response = fh
        .describe_delivery_stream()
        .delivery_stream_name(stream)
        .send()
        .await
        .unwrap();
    let description = response.delivery_stream_description().unwrap();
    fh.update_destination()
        .delivery_stream_name(stream)
        .current_delivery_stream_version_id(description.version_id())
        .destination_id(description.destinations()[0].destination_id())
        .extended_s3_destination_update(config)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn firehose_size_trigger_aggregates_separate_calls_decoded_before_compression() {
    // The independent decoder is checked against Java-produced fixtures.
    for (fixture, expected) in [
        (
            include_bytes!("../../fakecloud-firehose/tests/fixtures/xerial_binary.snappy")
                .as_slice(),
            xerial::binary_input(),
        ),
        (
            include_bytes!("../../fakecloud-firehose/tests/fixtures/xerial_multiblock.snappy")
                .as_slice(),
            xerial::multiblock_input(),
        ),
    ] {
        assert_eq!(xerial::decode(fixture).unwrap().concat(), expected);
    }
    let server = TestServer::start().await;
    let fh = server.firehose_client().await;
    let s3 = server.s3_client().await;
    for (i, codec, extension) in [
        (0, CompressionFormat::Uncompressed, ""),
        (1, CompressionFormat::Gzip, ".gz"),
        (2, CompressionFormat::Snappy, ".snappy"),
    ] {
        let bucket = format!("fh-size-{i}");
        s3.create_bucket().bucket(&bucket).send().await.unwrap();
        create(
            &fh,
            &bucket,
            destination_builder(&bucket, 300)
                .compression_format(codec.clone())
                .build()
                .unwrap(),
        )
        .await;
        let first = vec![b'a'; 512 * 1024];
        let second = vec![b'b'; 512 * 1024];
        put(&fh, &bucket, first.clone()).await;
        assert!(keys(&s3, &bucket).await.is_empty());
        put(&fh, &bucket, second.clone()).await;
        let keys = wait_keys(&s3, &bucket, 1).await;
        assert!(keys[0].ends_with(extension));
        assert_eq!(
            object(&s3, &bucket, &keys[0], codec.as_str()).await,
            [first, second].concat()
        );
        if i != 0 {
            let head = s3
                .head_object()
                .bucket(&bucket)
                .key(&keys[0])
                .send()
                .await
                .unwrap();
            assert!(head.content_length().unwrap() < 512 * 1024);
        }
    }
}

#[tokio::test]
async fn firehose_size_trigger_keeps_whole_batch_overshoot() {
    let server = TestServer::start().await;
    let fh = server.firehose_client().await;
    let s3 = server.s3_client().await;
    let bucket = "fh-overshoot";
    s3.create_bucket().bucket(bucket).send().await.unwrap();
    create(&fh, bucket, destination(bucket, 300)).await;
    put(&fh, bucket, vec![0; 256 * 1024]).await;
    assert!(keys(&s3, bucket).await.is_empty());
    let response = fh
        .put_record_batch()
        .delivery_stream_name(bucket)
        .records(record(vec![1; 400 * 1024]))
        .records(record(vec![2; 400 * 1024]))
        .records(record(vec![255; 400 * 1024]))
        .send()
        .await
        .unwrap();
    assert_eq!(response.failed_put_count(), 0);
    assert_eq!(response.request_responses().len(), 3);
    let keys = wait_keys(&s3, bucket, 1).await;
    assert_eq!(
        object(&s3, bucket, &keys[0], "UNCOMPRESSED").await,
        [
            vec![0; 256 * 1024],
            vec![1; 400 * 1024],
            vec![2; 400 * 1024],
            vec![255; 400 * 1024]
        ]
        .concat()
    );
}

#[tokio::test]
async fn firehose_interval_flushes_after_idle_and_starts_new_window() {
    let server = TestServer::start().await;
    let fh = server.firehose_client().await;
    let s3 = server.s3_client().await;
    let bucket = "fh-idle";
    s3.create_bucket().bucket(bucket).send().await.unwrap();
    create(&fh, bucket, destination(bucket, 2)).await;
    let before = chrono::Utc::now();
    put(&fh, bucket, b"first\x00\xff".to_vec()).await;
    let admitted = chrono::Utc::now();
    assert!(keys(&s3, bucket).await.is_empty());
    let first = wait_keys(&s3, bucket, 1).await;
    let delivered = chrono::Utc::now();
    let hierarchy = &first[0][..14];
    assert!(
        hierarchy == before.format("%Y/%m/%d/%H/").to_string()
            || hierarchy == admitted.format("%Y/%m/%d/%H/").to_string()
    );
    let suffix = first[0]
        .strip_prefix(&format!("{hierarchy}{bucket}-1-"))
        .unwrap();
    let timestamp = chrono::NaiveDateTime::parse_from_str(&suffix[..19], "%Y-%m-%d-%H-%M-%S")
        .unwrap()
        .and_utc();
    assert!((before.timestamp()..=delivered.timestamp()).contains(&timestamp.timestamp()));
    assert_eq!(
        object(&s3, bucket, &first[0], "UNCOMPRESSED").await,
        b"first\x00\xff"
    );
    put(&fh, bucket, b"second".to_vec()).await;
    assert_eq!(keys(&s3, bucket).await, first);
    let all = wait_keys(&s3, bucket, 2).await;
    let second = all.iter().find(|key| !first.contains(key)).unwrap();
    assert_eq!(object(&s3, bucket, second, "UNCOMPRESSED").await, b"second");
}

#[tokio::test]
async fn firehose_zero_interval_delivers_binary_batch_without_delimiters() {
    let server = TestServer::start().await;
    let fh = server.firehose_client().await;
    let s3 = server.s3_client().await;
    let bucket = "fh-zero";
    s3.create_bucket().bucket(bucket).send().await.unwrap();
    create(&fh, bucket, destination(bucket, 0)).await;
    put(&fh, bucket, Vec::new()).await;
    assert!(keys(&s3, bucket).await.is_empty());
    let ack = fh
        .put_record_batch()
        .delivery_stream_name(bucket)
        .records(record(b"abc".to_vec()))
        .records(record(b"def\n".to_vec()))
        .records(record(vec![0, 255]))
        .send()
        .await
        .unwrap();
    assert_eq!(ack.failed_put_count(), 0);
    let keys = wait_keys(&s3, bucket, 1).await;
    assert_eq!(
        object(&s3, bucket, &keys[0], "UNCOMPRESSED").await,
        b"abcdef\n\x00\xff"
    );
}

#[tokio::test]
async fn firehose_api_buffered_update_uses_latest_configuration() {
    let server = TestServer::start().await;
    let fh = server.firehose_client().await;
    let s3 = server.s3_client().await;
    for bucket in ["fh-before-update", "fh-after-update"] {
        s3.create_bucket().bucket(bucket).send().await.unwrap();
    }
    create(&fh, "updated", destination("fh-before-update", 300)).await;
    put(&fh, "updated", b"before".to_vec()).await;
    assert!(keys(&s3, "fh-before-update").await.is_empty());
    update(
        &fh,
        "updated",
        ExtendedS3DestinationUpdate::builder()
            .bucket_arn("arn:aws:s3:::fh-after-update")
            .prefix("events")
            .file_extension(".custom.log")
            .compression_format(CompressionFormat::Gzip)
            .buffering_hints(hints(1, 0))
            .build(),
    )
    .await;
    let delivered_keys = wait_keys(&s3, "fh-after-update", 1).await;
    assert!(
        delivered_keys[0].starts_with("events2"),
        "no slash inserted: {}",
        delivered_keys[0]
    );
    assert!(delivered_keys[0].contains("/updated-2-"));
    assert!(delivered_keys[0].ends_with(".custom.log"));
    assert_eq!(
        object(&s3, "fh-after-update", &delivered_keys[0], "GZIP").await,
        b"before"
    );
    assert!(keys(&s3, "fh-before-update").await.is_empty());
}

#[tokio::test]
async fn firehose_detached_config_survives_update_and_missing_bucket_creation() {
    let server = TestServer::start().await;
    let fh = server.firehose_client().await;
    let s3 = server.s3_client().await;
    create(
        &fh,
        "captured",
        destination_builder("fh-missing-old", 0)
            .prefix("old/")
            .compression_format(CompressionFormat::Gzip)
            .file_extension(".old")
            .build()
            .unwrap(),
    )
    .await;
    // Zero interval detaches at admission even though the bucket is missing.
    put(&fh, "captured", b"captured-old".to_vec()).await;
    s3.create_bucket()
        .bucket("fh-present-new")
        .send()
        .await
        .unwrap();
    update(
        &fh,
        "captured",
        ExtendedS3DestinationUpdate::builder()
            .bucket_arn("arn:aws:s3:::fh-present-new")
            .prefix("new/")
            .compression_format(CompressionFormat::Snappy)
            .file_extension(".new")
            .build(),
    )
    .await;
    put(&fh, "captured", b"latest-new".to_vec()).await;
    s3.create_bucket()
        .bucket("fh-missing-old")
        .send()
        .await
        .unwrap();
    let old = wait_keys(&s3, "fh-missing-old", 1).await;
    let new = wait_keys(&s3, "fh-present-new", 1).await;
    assert!(old[0].starts_with("old/"));
    assert!(old[0].contains("/captured-1-"));
    assert!(old[0].ends_with(".old"));
    assert!(new[0].starts_with("new/"));
    assert!(new[0].contains("/captured-2-"));
    assert!(new[0].ends_with(".new"));
    assert_eq!(
        object(&s3, "fh-missing-old", &old[0], "GZIP").await,
        b"captured-old"
    );
    assert_eq!(
        object(&s3, "fh-present-new", &new[0], "Snappy").await,
        b"latest-new"
    );
    // This proves eventual retained delivery, not a specific failed attempt.
}

#[tokio::test]
async fn firehose_delete_recreate_discards_old_pending_data() {
    let server = TestServer::start().await;
    let fh = server.firehose_client().await;
    let s3 = server.s3_client().await;
    let bucket = "fh-recreated";
    s3.create_bucket().bucket(bucket).send().await.unwrap();
    create(&fh, "reused", destination(bucket, 300)).await;
    put(&fh, "reused", b"discard-me".to_vec()).await;
    assert!(keys(&s3, bucket).await.is_empty());
    fh.delete_delivery_stream()
        .delivery_stream_name("reused")
        .send()
        .await
        .unwrap();
    create(&fh, "reused", destination(bucket, 0)).await;
    put(&fh, "reused", b"new-instance".to_vec()).await;
    let keys = wait_keys(&s3, bucket, 1).await;
    assert_eq!(
        object(&s3, bucket, &keys[0], "UNCOMPRESSED").await,
        b"new-instance"
    );
}

#[tokio::test]
async fn firehose_unsupported_preflight_501_before_ack_and_control_roundtrip() {
    let server = TestServer::start().await;
    let fh = server.firehose_client().await;
    let s3 = server.s3_client().await;
    let bucket = "fh-unsupported";
    s3.create_bucket().bucket(bucket).send().await.unwrap();
    for (i, config) in [
        destination_builder(bucket, 0)
            .compression_format(CompressionFormat::Zip)
            .build()
            .unwrap(),
        destination_builder(bucket, 0)
            .compression_format(CompressionFormat::HadoopSnappy)
            .build()
            .unwrap(),
        destination_builder(bucket, 0)
            .prefix("!{timestamp:yyyy}/")
            .build()
            .unwrap(),
        destination_builder(bucket, 0)
            .custom_time_zone("America/New_York")
            .build()
            .unwrap(),
        destination_builder(bucket, 0)
            .processing_configuration(ProcessingConfiguration::builder().enabled(true).build())
            .build()
            .unwrap(),
        destination_builder(bucket, 0)
            .dynamic_partitioning_configuration(
                DynamicPartitioningConfiguration::builder()
                    .enabled(true)
                    .build(),
            )
            .build()
            .unwrap(),
        destination_builder(bucket, 0)
            .data_format_conversion_configuration(
                DataFormatConversionConfiguration::builder()
                    .enabled(true)
                    .build(),
            )
            .build()
            .unwrap(),
    ]
    .into_iter()
    .enumerate()
    {
        let stream = format!("unsupported-{i}");
        create(&fh, &stream, config.clone()).await;
        let response = fh
            .describe_delivery_stream()
            .delivery_stream_name(&stream)
            .send()
            .await
            .unwrap();
        let description = response
            .delivery_stream_description()
            .unwrap()
            .destinations()[0]
            .extended_s3_destination_description()
            .unwrap();
        assert_eq!(
            description.compression_format(),
            config
                .compression_format()
                .unwrap_or(&CompressionFormat::Uncompressed)
        );
        assert_eq!(description.prefix(), config.prefix());
        assert_eq!(
            description.custom_time_zone(),
            config.custom_time_zone().or(Some("UTC"))
        );
        assert_eq!(
            description.processing_configuration(),
            config.processing_configuration()
        );
        assert_eq!(
            description.dynamic_partitioning_configuration(),
            config.dynamic_partitioning_configuration()
        );
        assert_eq!(
            description.data_format_conversion_configuration(),
            config.data_format_conversion_configuration()
        );
        let error = fh
            .put_record()
            .delivery_stream_name(&stream)
            .record(record(b"reject".to_vec()))
            .send()
            .await
            .unwrap_err();
        assert_eq!(error.raw_response().unwrap().status().as_u16(), 501);
        assert_eq!(
            error.as_service_error().unwrap().code(),
            Some("NotImplemented")
        );
        let error = fh
            .put_record_batch()
            .delivery_stream_name(&stream)
            .records(record(b"reject-batch".to_vec()))
            .send()
            .await
            .unwrap_err();
        assert_eq!(error.raw_response().unwrap().status().as_u16(), 501);
        assert_eq!(
            error.as_service_error().unwrap().code(),
            Some("NotImplemented")
        );
    }
    assert!(keys(&s3, bucket).await.is_empty());
}

#[tokio::test]
async fn firehose_unsupported_update_pauses_accepted_data_until_supported_again() {
    let server = TestServer::start().await;
    let fh = server.firehose_client().await;
    let s3 = server.s3_client().await;
    let bucket = "fh-paused";
    s3.create_bucket().bucket(bucket).send().await.unwrap();
    create(&fh, bucket, destination(bucket, 300)).await;
    put(&fh, bucket, b"accepted".to_vec()).await;
    update(
        &fh,
        bucket,
        ExtendedS3DestinationUpdate::builder()
            .compression_format(CompressionFormat::Zip)
            .buffering_hints(hints(1, 0))
            .build(),
    )
    .await;
    let error = fh
        .put_record()
        .delivery_stream_name(bucket)
        .record(record(b"rejected".to_vec()))
        .send()
        .await
        .unwrap_err();
    assert_eq!(error.raw_response().unwrap().status().as_u16(), 501);
    assert!(keys(&s3, bucket).await.is_empty());
    update(
        &fh,
        bucket,
        ExtendedS3DestinationUpdate::builder()
            .compression_format(CompressionFormat::Uncompressed)
            .build(),
    )
    .await;
    let keys = wait_keys(&s3, bucket, 1).await;
    assert_eq!(
        object(&s3, bucket, &keys[0], "UNCOMPRESSED").await,
        b"accepted"
    );
}

#[tokio::test]
async fn firehose_cloudformation_provisioned_stream_delivers_compressed_custom_extension() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    s3.create_bucket().bucket("fh-cfn").send().await.unwrap();
    let cfn = server.cloudformation_client().await;
    cfn.create_stack().stack_name("buffered-firehose").template_body(serde_json::json!({
        "Resources": {"Delivery": {"Type": "AWS::KinesisFirehose::DeliveryStream", "Properties": {
            "DeliveryStreamName": "cfn-buffered",
            "ExtendedS3DestinationConfiguration": {
                "RoleARN": "arn:aws:iam::123456789012:role/firehose", "BucketARN": "arn:aws:s3:::fh-cfn",
                "BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 0},
                "CompressionFormat": "GZIP", "Prefix": "events/", "FileExtension": ".cfn.log"
            }
        }}}
    }).to_string()).send().await.unwrap();
    let fh = server.firehose_client().await;
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let response = cfn
                .describe_stacks()
                .stack_name("buffered-firehose")
                .send()
                .await
                .unwrap();
            let status = response.stacks()[0].stack_status().unwrap().as_str();
            if status == "CREATE_COMPLETE" {
                break;
            }
            assert_eq!(status, "CREATE_IN_PROGRESS");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("CFN completion");
    put(&fh, "cfn-buffered", b"cfn\x00\xff\n".to_vec()).await;
    let keys = wait_keys(&s3, "fh-cfn", 1).await;
    assert!(keys[0].starts_with("events/"));
    assert!(keys[0].ends_with(".cfn.log"));
    assert_eq!(
        object(&s3, "fh-cfn", &keys[0], "GZIP").await,
        b"cfn\x00\xff\n"
    );
}

#[tokio::test]
async fn firehose_logs_and_api_share_one_size_buffer() {
    let server = TestServer::start().await;
    let fh = server.firehose_client().await;
    let s3 = server.s3_client().await;
    let bucket = "fh-shared";
    s3.create_bucket().bucket(bucket).send().await.unwrap();
    create(&fh, bucket, destination(bucket, 300)).await;
    let seed = vec![b'A'; 500 * 1024];
    fh.put_record_batch()
        .delivery_stream_name(bucket)
        .records(record(seed.clone()))
        .records(record(seed.clone()))
        .send()
        .await
        .unwrap();
    assert!(keys(&s3, bucket).await.is_empty());
    let logs = server.logs_client().await;
    logs.create_log_group()
        .log_group_name("buffer-source")
        .send()
        .await
        .unwrap();
    logs.create_log_stream()
        .log_group_name("buffer-source")
        .log_stream_name("events")
        .send()
        .await
        .unwrap();
    logs.put_subscription_filter()
        .log_group_name("buffer-source")
        .filter_name("to-firehose")
        .filter_pattern("")
        .destination_arn("arn:aws:firehose:us-east-1:123456789012:deliverystream/fh-shared")
        .send()
        .await
        .unwrap();
    let message = hex::encode(xerial::incompressible_input(70000));
    logs.put_log_events()
        .log_group_name("buffer-source")
        .log_stream_name("events")
        .log_events(
            aws_sdk_cloudwatchlogs::types::InputLogEvent::builder()
                .timestamp(chrono::Utc::now().timestamp_millis())
                .message(&message)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let keys = wait_keys(&s3, bucket, 1).await;
    let bytes = object(&s3, bucket, &keys[0], "UNCOMPRESSED").await;
    assert!(bytes.len() >= 1_048_576);
    assert_eq!(&bytes[..1_024_000], [seed.clone(), seed].concat());
    let envelope = base64::engine::general_purpose::STANDARD
        .decode(&bytes[1_024_000..])
        .unwrap();
    let event: serde_json::Value = serde_json::from_slice(&gunzip(&envelope)).unwrap();
    assert_eq!(event["messageType"], "DATA_MESSAGE");
    assert_eq!(event["logGroup"], "buffer-source");
    assert_eq!(event["logEvents"][0]["message"], message);
}
