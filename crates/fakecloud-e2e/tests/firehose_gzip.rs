use std::io::Read;

use aws_sdk_firehose::primitives::Blob;
use aws_sdk_firehose::types::{
    CompressionFormat, ExtendedS3DestinationConfiguration, Record, S3DestinationConfiguration,
};
use base64::Engine;
use fakecloud_testkit::TestServer;

fn decode_gzip(raw: &[u8]) -> Vec<u8> {
    assert!(
        raw.starts_with(&[0x1f, 0x8b, 8]),
        "S3 must return actual GZIP bytes"
    );
    let mut decoded = Vec::new();
    flate2::read::GzDecoder::new(raw)
        .read_to_end(&mut decoded)
        .unwrap();
    decoded
}

async fn gzip_object(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    extension: &str,
) -> (String, Vec<u8>, Vec<u8>) {
    let listing = s3
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .send()
        .await
        .unwrap();
    assert_eq!(listing.contents().len(), 1);
    assert_eq!(
        listing.contents()[0]
            .storage_class()
            .map(|value| value.as_str()),
        Some("STANDARD")
    );
    let key = listing.contents()[0].key().unwrap().to_owned();
    assert!(
        key.ends_with(extension),
        "key {key} must end with {extension}"
    );
    let object = s3
        .get_object()
        .bucket(bucket)
        .key(&key)
        .send()
        .await
        .unwrap();
    assert_eq!(object.content_type(), Some("application/octet-stream"));
    assert_eq!(object.content_encoding(), Some("gzip"));
    let content_length = object.content_length().unwrap();
    let raw = object.body.collect().await.unwrap().into_bytes().to_vec();
    assert_eq!(content_length, raw.len() as i64);
    let decoded = decode_gzip(&raw);
    (key, raw, decoded)
}

#[tokio::test]
async fn firehose_gzip_batch_preserves_bytes_and_extension_overrides() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let firehose = server.firehose_client().await;

    for (i, (extension, expected_extension)) in [
        (None, ".gz"),
        (Some(""), ".gz"),
        (Some(".custom.log"), ".custom.log"),
        (None, ".gz"),
    ]
    .into_iter()
    .enumerate()
    {
        let bucket = format!("fh-gzip-{i}");
        let stream = format!("gzip-{i}");
        s3.create_bucket().bucket(&bucket).send().await.unwrap();
        let create = firehose
            .create_delivery_stream()
            .delivery_stream_name(&stream);
        let create = if i == 3 {
            // The legacy S3 configuration is deprecated by AWS but still supported.
            #[allow(deprecated)]
            let legacy = create.s3_destination_configuration(
                S3DestinationConfiguration::builder()
                    .role_arn("arn:aws:iam::123456789012:role/firehose")
                    .bucket_arn(format!("arn:aws:s3:::{bucket}"))
                    .compression_format(CompressionFormat::Gzip)
                    .build()
                    .unwrap(),
            );
            legacy
        } else {
            create.extended_s3_destination_configuration(
                ExtendedS3DestinationConfiguration::builder()
                    .role_arn("arn:aws:iam::123456789012:role/firehose")
                    .bucket_arn(format!("arn:aws:s3:::{bucket}"))
                    .compression_format(CompressionFormat::Gzip)
                    .set_file_extension(extension.map(str::to_owned))
                    .build()
                    .unwrap(),
            )
        };
        create.send().await.unwrap();
        let response = firehose
            .put_record_batch()
            .delivery_stream_name(&stream)
            .records(Record::builder().data(Blob::new(b"abc")).build().unwrap())
            .records(Record::builder().data(Blob::new(b"def\n")).build().unwrap())
            .records(Record::builder().data(Blob::new([0, 255])).build().unwrap())
            .send()
            .await
            .unwrap();
        assert_eq!(response.failed_put_count(), 0);
        assert_eq!(response.request_responses().len(), 3);
        let (_, _, decoded) = gzip_object(&s3, &bucket, "", expected_extension).await;
        assert_eq!(decoded, b"abcdef\n\x00\xff");
    }
}

#[tokio::test]
async fn firehose_gzip_public_and_internal_objects_survive_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let s3 = server.s3_client().await;
    let firehose = server.firehose_client().await;
    let bucket = "fh-gzip-persistent";
    s3.create_bucket().bucket(bucket).send().await.unwrap();
    for (stream, prefix) in [("gzip-direct", "direct/"), ("gzip-hook", "hook/")] {
        firehose
            .create_delivery_stream()
            .delivery_stream_name(stream)
            .extended_s3_destination_configuration(
                ExtendedS3DestinationConfiguration::builder()
                    .role_arn("arn:aws:iam::123456789012:role/firehose")
                    .bucket_arn(format!("arn:aws:s3:::{bucket}"))
                    .prefix(prefix)
                    .compression_format(CompressionFormat::Gzip)
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();
    }
    firehose
        .put_record()
        .delivery_stream_name("gzip-direct")
        .record(
            Record::builder()
                .data(Blob::new(b"\x00\xffpersist\n"))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Official Logs calls exercise the internal Firehose adapter and its late
    // store wiring, rather than passing through the public Firehose record API.
    let logs = server.logs_client().await;
    logs.create_log_group()
        .log_group_name("gzip-source")
        .send()
        .await
        .unwrap();
    logs.create_log_stream()
        .log_group_name("gzip-source")
        .log_stream_name("events")
        .send()
        .await
        .unwrap();
    logs.put_subscription_filter()
        .log_group_name("gzip-source")
        .filter_name("to-gzip")
        .filter_pattern("")
        .destination_arn("arn:aws:firehose:us-east-1:123456789012:deliverystream/gzip-hook")
        .send()
        .await
        .unwrap();
    logs.put_log_events()
        .log_group_name("gzip-source")
        .log_stream_name("events")
        .log_events(
            aws_sdk_cloudwatchlogs::types::InputLogEvent::builder()
                .timestamp(chrono::Utc::now().timestamp_millis())
                .message("hook-event")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let direct_before = gzip_object(&s3, bucket, "direct/", ".gz").await;
    assert_eq!(direct_before.2, b"\x00\xffpersist\n");
    let hook_before = gzip_object(&s3, bucket, "hook/", ".gz").await;
    // The adapter's existing Logs payload is a base64-encoded GZIP envelope.
    // The Firehose GZIP layer must preserve those producer bytes verbatim.
    let envelope = base64::engine::general_purpose::STANDARD
        .decode(&hook_before.2)
        .unwrap();
    let event: serde_json::Value = serde_json::from_slice(&decode_gzip(&envelope)).unwrap();
    assert_eq!(event["messageType"], "DATA_MESSAGE");
    assert_eq!(event["logGroup"], "gzip-source");
    assert_eq!(event["logStream"], "events");
    assert_eq!(event["logEvents"][0]["message"], "hook-event");

    server.restart().await;
    let s3 = server.s3_client().await;
    assert_eq!(
        gzip_object(&s3, bucket, "direct/", ".gz").await,
        direct_before
    );
    assert_eq!(gzip_object(&s3, bucket, "hook/", ".gz").await, hook_before);
}
