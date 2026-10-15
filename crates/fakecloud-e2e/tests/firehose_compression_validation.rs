use aws_sdk_firehose::error::ProvideErrorMetadata;
use aws_sdk_firehose::types::{
    CompressionFormat, ExtendedS3DestinationConfiguration, ExtendedS3DestinationUpdate,
    S3DestinationUpdate,
};
use fakecloud_testkit::TestServer;

fn destination(format: &str) -> ExtendedS3DestinationConfiguration {
    ExtendedS3DestinationConfiguration::builder()
        .role_arn("arn:aws:iam::123456789012:role/firehose")
        .bucket_arn("arn:aws:s3:::compression-bucket")
        .prefix("before/")
        .compression_format(CompressionFormat::from(format))
        .build()
        .unwrap()
}

async fn firehose_cfn_invalid_retry(change_set: bool) {
    use aws_sdk_cloudformation::types::{ChangeSetType, Parameter, Tag};
    use serde_json::json;

    let server = TestServer::start().await;
    let cfn = server.cloudformation_client().await;
    let firehose = server.firehose_client().await;
    let template = json!({
        "Parameters": {
            "Codec": {"Type": "String", "Default": "UNCOMPRESSED"},
            "Prefix": {"Type": "String", "Default": "default/"}
        },
        "Resources": {"Stream": {
            "Type": "AWS::KinesisFirehose::DeliveryStream",
            "Properties": {
                "DeliveryStreamName": "sdk-cfn-compression",
                "ExtendedS3DestinationConfiguration": {
                    "RoleARN": "arn:aws:iam::123456789012:role/fh",
                    "BucketARN": "arn:aws:s3:::bucket",
                    "Prefix": {"Ref": "Prefix"},
                    "CompressionFormat": {"Ref": "Codec"}
                }
            }
        }},
        "Outputs": {"Codec": {"Value": {"Ref": "Codec"}}}
    });
    let original = template.to_string();
    cfn.create_stack()
        .stack_name("compression-stack")
        .template_body(&original)
        .parameters(
            Parameter::builder()
                .parameter_key("Codec")
                .parameter_value("GZIP")
                .build(),
        )
        .parameters(
            Parameter::builder()
                .parameter_key("Prefix")
                .parameter_value("before/")
                .build(),
        )
        .tags(Tag::builder().key("env").value("before").build())
        .send()
        .await
        .unwrap();
    let before = cfn
        .describe_stacks()
        .stack_name("compression-stack")
        .send()
        .await
        .unwrap();
    assert_eq!(
        before.stacks()[0].stack_status().unwrap().as_str(),
        "CREATE_COMPLETE"
    );
    let firehose_before = firehose
        .describe_delivery_stream()
        .delivery_stream_name("sdk-cfn-compression")
        .send()
        .await
        .unwrap();
    let mut invalid = template.clone();
    invalid["Resources"]["Stream"]["Properties"]["ExtendedS3DestinationConfiguration"]
        ["CompressionFormat"] = json!("BROTLI");
    let invalid = invalid.to_string();
    for name in ["invalid-1", "invalid-2"] {
        let parameters = vec![
            Parameter::builder()
                .parameter_key("Codec")
                .parameter_value("ZIP")
                .build(),
            Parameter::builder()
                .parameter_key("Prefix")
                .use_previous_value(true)
                .build(),
        ];
        if change_set {
            cfn.create_change_set()
                .stack_name("compression-stack")
                .change_set_name(name)
                .change_set_type(ChangeSetType::Update)
                .template_body(&invalid)
                .set_parameters(Some(parameters))
                .tags(Tag::builder().key("env").value("rejected").build())
                .send()
                .await
                .unwrap();
            let err = cfn
                .execute_change_set()
                .stack_name("compression-stack")
                .change_set_name(name)
                .send()
                .await
                .expect_err("invalid change set must fail even on retry");
            assert_eq!(err.raw_response().unwrap().status().as_u16(), 400);
        } else {
            let err = cfn
                .update_stack()
                .stack_name("compression-stack")
                .template_body(&invalid)
                .set_parameters(Some(parameters))
                .tags(Tag::builder().key("env").value("rejected").build())
                .send()
                .await
                .expect_err("invalid update must fail even on retry");
            assert_eq!(err.raw_response().unwrap().status().as_u16(), 400);
        }
        let after = cfn
            .describe_stacks()
            .stack_name("compression-stack")
            .send()
            .await
            .unwrap();
        let stack = &after.stacks()[0];
        assert_eq!(
            stack.stack_status().unwrap().as_str(),
            "UPDATE_ROLLBACK_COMPLETE"
        );
        assert!(stack
            .stack_status_reason()
            .unwrap()
            .contains("CompressionFormat"));
        assert_eq!(stack.parameters(), before.stacks()[0].parameters());
        assert_eq!(stack.tags(), before.stacks()[0].tags());
        assert_eq!(stack.outputs(), before.stacks()[0].outputs());
        let stored = cfn
            .get_template()
            .stack_name("compression-stack")
            .send()
            .await
            .unwrap();
        assert_eq!(stored.template_body(), Some(original.as_str()));
        let firehose_after = firehose
            .describe_delivery_stream()
            .delivery_stream_name("sdk-cfn-compression")
            .send()
            .await
            .unwrap();
        assert_eq!(
            firehose_after.delivery_stream_description(),
            firehose_before.delivery_stream_description()
        );
    }

    let parameters = vec![
        Parameter::builder()
            .parameter_key("Codec")
            .parameter_value("Snappy")
            .build(),
        Parameter::builder()
            .parameter_key("Prefix")
            .use_previous_value(true)
            .build(),
    ];
    if change_set {
        cfn.create_change_set()
            .stack_name("compression-stack")
            .change_set_name("valid")
            .change_set_type(ChangeSetType::Update)
            .template_body(&original)
            .set_parameters(Some(parameters))
            .send()
            .await
            .unwrap();
        cfn.execute_change_set()
            .stack_name("compression-stack")
            .change_set_name("valid")
            .send()
            .await
            .unwrap();
    } else {
        cfn.update_stack()
            .stack_name("compression-stack")
            .template_body(&original)
            .set_parameters(Some(parameters))
            .send()
            .await
            .unwrap();
    }
    let after = cfn
        .describe_stacks()
        .stack_name("compression-stack")
        .send()
        .await
        .unwrap();
    let stack = &after.stacks()[0];
    assert_eq!(stack.stack_status().unwrap().as_str(), "UPDATE_COMPLETE");
    assert!(stack.stack_status_reason().is_none());
    assert_eq!(stack.outputs()[0].output_value(), Some("Snappy"));
    let stored = cfn
        .get_template()
        .stack_name("compression-stack")
        .send()
        .await
        .unwrap();
    assert_eq!(stored.template_body(), Some(original.as_str()));
    assert_eq!(
        stack
            .parameters()
            .iter()
            .find(|p| p.parameter_key() == Some("Codec"))
            .unwrap()
            .parameter_value(),
        Some("Snappy")
    );
    assert_eq!(
        stack
            .parameters()
            .iter()
            .find(|p| p.parameter_key() == Some("Prefix"))
            .unwrap()
            .parameter_value(),
        Some("before/")
    );
    let desc = firehose
        .describe_delivery_stream()
        .delivery_stream_name("sdk-cfn-compression")
        .send()
        .await
        .unwrap();
    let stream = desc.delivery_stream_description().unwrap();
    assert_eq!(stream.version_id(), "2");
    let dest = stream.destinations()[0]
        .extended_s3_destination_description()
        .unwrap();
    assert_eq!(dest.compression_format().as_str(), "Snappy");
    assert_eq!(dest.prefix(), Some("before/"));
}

#[tokio::test]
async fn firehose_cfn_update_stack_rejects_invalid_retry() {
    firehose_cfn_invalid_retry(false).await;
}

#[tokio::test]
async fn firehose_cfn_execute_change_set_rejects_invalid_retry() {
    firehose_cfn_invalid_retry(true).await;
}

#[tokio::test]
async fn firehose_sdk_unknown_compression_rejected_without_mutation() {
    let server = TestServer::start().await;
    let client = server.firehose_client().await;
    let err = client
        .create_delivery_stream()
        .delivery_stream_name("invalid-compression")
        .extended_s3_destination_configuration(destination("BROTLI"))
        .send()
        .await
        .expect_err("SDK Unknown must reach server validation");
    assert_eq!(err.raw_response().unwrap().status().as_u16(), 400);
    assert_eq!(
        err.as_service_error().unwrap().code(),
        Some("InvalidArgumentException")
    );
    assert!(client
        .list_delivery_streams()
        .send()
        .await
        .unwrap()
        .delivery_stream_names()
        .is_empty());

    client
        .create_delivery_stream()
        .delivery_stream_name("compression")
        .extended_s3_destination_configuration(destination("GZIP"))
        .send()
        .await
        .unwrap();
    let before = client
        .describe_delivery_stream()
        .delivery_stream_name("compression")
        .send()
        .await
        .unwrap();
    let desc = before.delivery_stream_description().unwrap();
    for update in [
        ExtendedS3DestinationUpdate::builder()
            .prefix("changed/")
            .compression_format(CompressionFormat::from("BROTLI"))
            .build(),
        ExtendedS3DestinationUpdate::builder()
            .prefix("changed/")
            .s3_backup_update(
                S3DestinationUpdate::builder()
                    .compression_format(CompressionFormat::from("BROTLI"))
                    .build(),
            )
            .build(),
    ] {
        let err = client
            .update_destination()
            .delivery_stream_name("compression")
            .current_delivery_stream_version_id(desc.version_id())
            .destination_id(desc.destinations()[0].destination_id())
            .extended_s3_destination_update(update)
            .send()
            .await
            .expect_err("invalid update must fail");
        assert_eq!(err.raw_response().unwrap().status().as_u16(), 400);
        assert_eq!(
            err.as_service_error().unwrap().code(),
            Some("InvalidArgumentException")
        );
        let after = client
            .describe_delivery_stream()
            .delivery_stream_name("compression")
            .send()
            .await
            .unwrap();
        assert_eq!(
            after.delivery_stream_description(),
            before.delivery_stream_description()
        );
    }
}

#[tokio::test]
async fn firehose_sdk_all_valid_compression_values_round_trip() {
    let server = TestServer::start().await;
    let client = server.firehose_client().await;
    for (format, updated_format) in [
        ("UNCOMPRESSED", "GZIP"),
        ("GZIP", "ZIP"),
        ("ZIP", "Snappy"),
        ("Snappy", "HADOOP_SNAPPY"),
        ("HADOOP_SNAPPY", "UNCOMPRESSED"),
    ] {
        let name = format!("compression-{format}");
        client
            .create_delivery_stream()
            .delivery_stream_name(&name)
            .extended_s3_destination_configuration(destination(format))
            .send()
            .await
            .unwrap();
        let before = client
            .describe_delivery_stream()
            .delivery_stream_name(&name)
            .send()
            .await
            .unwrap();
        let desc = before.delivery_stream_description().unwrap();
        assert_eq!(
            desc.destinations()[0]
                .extended_s3_destination_description()
                .unwrap()
                .compression_format()
                .as_str(),
            format
        );
        client
            .update_destination()
            .delivery_stream_name(&name)
            .current_delivery_stream_version_id(desc.version_id())
            .destination_id(desc.destinations()[0].destination_id())
            .extended_s3_destination_update(
                ExtendedS3DestinationUpdate::builder()
                    .compression_format(CompressionFormat::from(updated_format))
                    .prefix("after/")
                    .build(),
            )
            .send()
            .await
            .unwrap();
        let after = client
            .describe_delivery_stream()
            .delivery_stream_name(&name)
            .send()
            .await
            .unwrap();
        let desc = after.delivery_stream_description().unwrap();
        assert_eq!(desc.version_id(), "2");
        let s3 = desc.destinations()[0]
            .extended_s3_destination_description()
            .unwrap();
        assert_eq!(s3.compression_format().as_str(), updated_format);
        assert_eq!(s3.prefix(), Some("after/"));
    }
}
