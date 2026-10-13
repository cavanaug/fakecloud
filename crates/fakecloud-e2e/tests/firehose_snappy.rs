use aws_sdk_firehose::primitives::Blob;
use aws_sdk_firehose::types::{CompressionFormat, ExtendedS3DestinationConfiguration, Record};
use fakecloud_testkit::TestServer;

#[path = "../../fakecloud-firehose/tests/fixtures/xerial_decoder.rs"]
mod xerial;

async fn snappy_object(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    extension: &str,
) -> (String, Vec<u8>, Vec<u8>) {
    let listing = s3.list_objects_v2().bucket(bucket).send().await.unwrap();
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
    assert_eq!(object.content_encoding(), Some("snappy-java"));
    let content_length = object.content_length().unwrap();
    let raw = object.body.collect().await.unwrap().into_bytes().to_vec();
    assert_eq!(content_length, raw.len() as i64);
    let decoded = xerial::decode(&raw).unwrap().concat();
    (key, raw, decoded)
}

#[tokio::test]
async fn firehose_snappy_binary_multiblock_and_extension_overrides() {
    // Validate this test's separate decoder against Java-produced streams before
    // using it to judge output fetched through the official AWS SDK.
    assert_eq!(
        xerial::decode(include_bytes!(
            "../../fakecloud-firehose/tests/fixtures/xerial_binary.snappy"
        ))
        .unwrap()
        .concat(),
        xerial::binary_input()
    );
    assert_eq!(
        xerial::decode(include_bytes!(
            "../../fakecloud-firehose/tests/fixtures/xerial_multiblock.snappy"
        ))
        .unwrap()
        .concat(),
        xerial::multiblock_input()
    );

    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let firehose = server.firehose_client().await;
    for (i, (extension, expected_extension, input)) in [
        (None, ".snappy", xerial::binary_input()),
        (Some(""), ".snappy", xerial::multiblock_input()),
        (
            Some(".custom.log"),
            ".custom.log",
            xerial::incompressible_input(70000),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let bucket = format!("fh-snappy-{i}");
        let stream = format!("snappy-{i}");
        s3.create_bucket().bucket(&bucket).send().await.unwrap();
        firehose
            .create_delivery_stream()
            .delivery_stream_name(&stream)
            .extended_s3_destination_configuration(
                ExtendedS3DestinationConfiguration::builder()
                    .role_arn("arn:aws:iam::123456789012:role/firehose")
                    .bucket_arn(format!("arn:aws:s3:::{bucket}"))
                    .compression_format(CompressionFormat::Snappy)
                    .set_file_extension(extension.map(str::to_owned))
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();
        let split = if input.len() > 32768 { 32760 } else { 17 };
        let response = firehose
            .put_record_batch()
            .delivery_stream_name(&stream)
            .records(
                Record::builder()
                    .data(Blob::new(&input[..split]))
                    .build()
                    .unwrap(),
            )
            .records(
                Record::builder()
                    .data(Blob::new(&input[split..]))
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();
        assert_eq!(response.failed_put_count(), 0);
        assert_eq!(response.request_responses().len(), 2);
        let (_, raw, decoded) = snappy_object(&s3, &bucket, expected_extension).await;
        assert_eq!(decoded, input);
        let blocks = xerial::decode(&raw).unwrap();
        let expected_sizes: Vec<usize> = input.chunks(32768).map(<[u8]>::len).collect();
        assert_eq!(
            blocks.iter().map(Vec::len).collect::<Vec<_>>(),
            expected_sizes
        );
        if i == 2 {
            assert!(
                raw.len() > input.len(),
                "incompressible blocks can expand without losing data"
            );
        }
    }
}

#[tokio::test]
async fn firehose_snappy_object_and_configuration_survive_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let s3 = server.s3_client().await;
    let firehose = server.firehose_client().await;
    let bucket = "fh-snappy-persistent";
    s3.create_bucket().bucket(bucket).send().await.unwrap();
    firehose
        .create_delivery_stream()
        .delivery_stream_name("snappy-durable")
        .extended_s3_destination_configuration(
            ExtendedS3DestinationConfiguration::builder()
                .role_arn("arn:aws:iam::123456789012:role/firehose")
                .bucket_arn(format!("arn:aws:s3:::{bucket}"))
                .prefix("events/")
                .compression_format(CompressionFormat::Snappy)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let input = xerial::incompressible_input(70000);
    firehose
        .put_record()
        .delivery_stream_name("snappy-durable")
        .record(
            Record::builder()
                .data(Blob::new(input.clone()))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let before = snappy_object(&s3, bucket, ".snappy").await;
    assert_eq!(before.2, input);
    server.restart().await;
    let s3 = server.s3_client().await;
    assert_eq!(snappy_object(&s3, bucket, ".snappy").await, before);
    let firehose = server.firehose_client().await;
    let response = firehose
        .describe_delivery_stream()
        .delivery_stream_name("snappy-durable")
        .send()
        .await
        .unwrap();
    let destination = response
        .delivery_stream_description()
        .unwrap()
        .destinations()[0]
        .extended_s3_destination_description()
        .unwrap();
    assert_eq!(destination.compression_format(), &CompressionFormat::Snappy);
}
