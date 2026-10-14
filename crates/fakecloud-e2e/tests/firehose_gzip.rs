use std::io::Read;

use aws_sdk_firehose::primitives::Blob;
use aws_sdk_firehose::types::{
    BufferingHints, CompressionFormat, ExtendedS3DestinationConfiguration, ProcessingConfiguration,
    Processor, ProcessorType, Record, S3DestinationConfiguration,
};
use fakecloud_testkit::TestServer;
use md5::{Digest, Md5};

/// Decode exactly one GZIP member: a second member or trailing bytes after the
/// first member's trailer fail the assertion.
fn decode_gzip(raw: &[u8]) -> Vec<u8> {
    assert!(
        raw.starts_with(&[0x1f, 0x8b, 8]),
        "S3 must return actual GZIP bytes"
    );
    let mut decoder = flate2::bufread::GzDecoder::new(raw);
    let mut decoded = Vec::new();
    decoder.read_to_end(&mut decoded).unwrap();
    assert!(decoder.into_inner().is_empty(), "expected one GZIP member");
    decoded
}

async fn gzip_object(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
    extension: &str,
) -> (String, Vec<u8>, Vec<u8>) {
    let listing = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let listing = s3
                .list_objects_v2()
                .bucket(bucket)
                .prefix(prefix)
                .send()
                .await
                .unwrap();
            if !listing.contents().is_empty() {
                return listing;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Firehose GZIP object within 10s");
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
    let etag = object.e_tag().map(str::to_owned);
    let raw = object.body.collect().await.unwrap().into_bytes().to_vec();
    assert_eq!(content_length, raw.len() as i64);
    // Size and ETag describe the stored compressed bytes, like any
    // single-part S3 object.
    let md5_etag = format!("\"{:x}\"", Md5::digest(&raw));
    assert_eq!(etag.as_deref(), Some(md5_etag.as_str()));
    assert_eq!(listing.contents()[0].e_tag(), Some(md5_etag.as_str()));
    assert_eq!(listing.contents()[0].size(), Some(content_length));

    let head = s3
        .head_object()
        .bucket(bucket)
        .key(&key)
        .send()
        .await
        .unwrap();
    assert_eq!(head.content_type(), Some("application/octet-stream"));
    assert_eq!(head.content_encoding(), Some("gzip"));
    assert_eq!(head.content_length(), Some(content_length));
    assert_eq!(head.e_tag(), Some(md5_etag.as_str()));

    // S3 never decodes the object: a ranged read returns the stored GZIP
    // header bytes, still labelled with the object's content encoding.
    let ranged = s3
        .get_object()
        .bucket(bucket)
        .key(&key)
        .range("bytes=0-2")
        .send()
        .await
        .unwrap();
    assert_eq!(ranged.content_encoding(), Some("gzip"));
    assert_eq!(
        ranged.body.collect().await.unwrap().into_bytes().as_ref(),
        &[0x1f, 0x8b, 8]
    );

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
                    .buffering_hints(
                        BufferingHints::builder()
                            .size_in_mbs(5)
                            .interval_in_seconds(0)
                            .build(),
                    )
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
                    .buffering_hints(
                        BufferingHints::builder()
                            .size_in_mbs(5)
                            .interval_in_seconds(0)
                            .build(),
                    )
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

/// `AppendDelimiterToRecord` adds `\n` after every record before the payload
/// is compressed, so the delimiters live inside the GZIP stream.
#[tokio::test]
async fn firehose_gzip_compresses_records_after_append_delimiter() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let firehose = server.firehose_client().await;
    let bucket = "fh-gzip-delimited";
    s3.create_bucket().bucket(bucket).send().await.unwrap();
    firehose
        .create_delivery_stream()
        .delivery_stream_name("gzip-delimited")
        .extended_s3_destination_configuration(
            ExtendedS3DestinationConfiguration::builder()
                .role_arn("arn:aws:iam::123456789012:role/firehose")
                .bucket_arn(format!("arn:aws:s3:::{bucket}"))
                // Zero interval: the batch is due at admission; the helper
                // polls for the asynchronous write.
                .buffering_hints(
                    BufferingHints::builder()
                        .size_in_mbs(5)
                        .interval_in_seconds(0)
                        .build(),
                )
                .compression_format(CompressionFormat::Gzip)
                .processing_configuration(
                    ProcessingConfiguration::builder()
                        .enabled(true)
                        .processors(
                            Processor::builder()
                                .r#type(ProcessorType::AppendDelimiterToRecord)
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let response = firehose
        .put_record_batch()
        .delivery_stream_name("gzip-delimited")
        .records(Record::builder().data(Blob::new(b"a")).build().unwrap())
        .records(Record::builder().data(Blob::new(b"b\n")).build().unwrap())
        .records(Record::builder().data(Blob::new([0, 255])).build().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(response.failed_put_count(), 0);
    let (_, _, decoded) = gzip_object(&s3, bucket, "", ".gz").await;
    assert_eq!(decoded, b"a\nb\n\n\x00\xff\n");
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
                    .buffering_hints(
                        BufferingHints::builder()
                            .size_in_mbs(5)
                            .interval_in_seconds(0)
                            .build(),
                    )
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
    // A Logs record is already a GZIP-compressed envelope, so a GZIP stream
    // compresses it a second time, as on AWS. The Firehose layer must preserve
    // that producer GZIP member verbatim inside its own.
    let event: serde_json::Value = serde_json::from_slice(&decode_gzip(&hook_before.2)).unwrap();
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
