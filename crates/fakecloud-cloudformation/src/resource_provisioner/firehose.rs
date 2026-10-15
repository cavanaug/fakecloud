//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `firehose`.

use super::*;

/// CFN currently provisions the classic and Extended S3 configurations. Check
/// their enum inputs, including Extended S3's backup, before parsing or locking
/// state; the CFN parser otherwise silently discards non-string enum values.
fn validate_firehose_compression(props: &serde_json::Value) -> Result<(), String> {
    for member in [
        "S3DestinationConfiguration",
        "ExtendedS3DestinationConfiguration",
    ] {
        fakecloud_firehose::validate_s3_compression(&props[member])
            .map_err(|err| err.to_string())?;
    }
    fakecloud_firehose::validate_s3_compression(
        &props["ExtendedS3DestinationConfiguration"]["S3BackupConfiguration"],
    )
    .map_err(|err| err.to_string())
}

impl ResourceProvisioner {
    // --- Firehose ---

    pub(super) fn create_firehose_delivery_stream(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        validate_firehose_compression(props)?;
        let generated_name = self.physical_name(resource);
        let name = props
            .get("DeliveryStreamName")
            .and_then(|v| v.as_str())
            .unwrap_or(&generated_name)
            .to_string();

        let arn = self.regional_arn("firehose", &format!("deliverystream/{}", name));
        let stream_type = props
            .get("DeliveryStreamType")
            .and_then(|v| v.as_str())
            .unwrap_or("DirectPut")
            .to_string();

        let has_s3 = props.get("S3DestinationConfiguration").is_some();
        let has_extended_s3 = props.get("ExtendedS3DestinationConfiguration").is_some();
        if has_s3 && has_extended_s3 {
            return Err("Only one of S3DestinationConfiguration or ExtendedS3DestinationConfiguration may be set".to_string());
        }
        let destination = Some(if let Some(s3) = props.get("S3DestinationConfiguration") {
            parse_firehose_s3_destination(s3)?
        } else if let Some(s3) = props.get("ExtendedS3DestinationConfiguration") {
            parse_firehose_s3_destination(s3)?
        } else {
            return Err("Delivery stream requires a destination configuration".to_string());
        });

        let mut tags = BTreeMap::new();
        if let Some(arr) = props.get("Tags").and_then(|v| v.as_array()) {
            for tag in arr {
                if let (Some(k), Some(v)) = (
                    tag.get("Key").and_then(|v| v.as_str()),
                    tag.get("Value").and_then(|v| v.as_str()),
                ) {
                    tags.insert(k.to_string(), v.to_string());
                }
            }
        }

        let stream = DeliveryStream {
            instance_id: uuid::Uuid::new_v4(),
            name: name.clone(),
            arn: arn.clone(),
            status: "ACTIVE".to_string(),
            stream_type: stream_type.clone(),
            created_at: Utc::now(),
            last_update: Utc::now(),
            version_id: "1".to_string(),
            destination,
            tags,
            encryption: None,
            extra_destinations: std::collections::BTreeMap::new(),
        };

        let mut state = self.firehose_state.write();
        let account = state.get_or_create(&self.account_id, &self.region);
        account
            .streams_mut(&self.region)
            .insert(name.clone(), stream);

        let mut attributes = BTreeMap::new();
        attributes.insert("Arn".to_string(), arn.clone());
        attributes.insert("DeliveryStreamName".to_string(), name.clone());

        Ok(ProvisionResult {
            physical_id: name,
            attributes,
        })
    }

    /// In-place `UpdateStack` for an `AWS::KinesisFirehose::DeliveryStream`.
    /// Mutates the stored `DeliveryStream` in place instead of the reprovision
    /// fallback's delete+recreate. Applies the mutable destination config + tags
    /// (matching `UpdateDestination`/tag ops), bumps the `version_id`, and
    /// preserves the identity (`arn`, `created_at`, `stream_type`) and any
    /// `extra_destinations` on the record.
    pub(super) fn update_firehose_delivery_stream(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        validate_firehose_compression(props)?;
        let name = existing.physical_id.clone();

        let has_s3 = props.get("S3DestinationConfiguration").is_some();
        let has_extended_s3 = props.get("ExtendedS3DestinationConfiguration").is_some();
        if has_s3 && has_extended_s3 {
            return Err("Only one of S3DestinationConfiguration or ExtendedS3DestinationConfiguration may be set".to_string());
        }
        let new_destination = if let Some(s3) = props.get("S3DestinationConfiguration") {
            Some(parse_firehose_s3_destination(s3)?)
        } else if let Some(s3) = props.get("ExtendedS3DestinationConfiguration") {
            Some(parse_firehose_s3_destination(s3)?)
        } else {
            None
        };

        let mut state = self.firehose_state.write();
        let account = state.get_or_create(&self.account_id, &self.region);
        let stream = account
            .streams_mut(&self.region)
            .get_mut(&name)
            .ok_or_else(|| format!("Delivery stream {name} not yet provisioned"))?;

        if let Some(mut dest) = new_destination {
            if let Some(existing) = &stream.destination {
                dest.destination_id = existing.destination_id.clone();
            }
            stream.destination = Some(dest);
        }
        if props.get("Tags").is_some() {
            let mut tags = BTreeMap::new();
            if let Some(arr) = props.get("Tags").and_then(|v| v.as_array()) {
                for tag in arr {
                    if let (Some(k), Some(v)) = (
                        tag.get("Key").and_then(|v| v.as_str()),
                        tag.get("Value").and_then(|v| v.as_str()),
                    ) {
                        tags.insert(k.to_string(), v.to_string());
                    }
                }
            }
            stream.tags = tags;
        }
        stream.last_update = Utc::now();
        let next_version = stream.version_id.parse::<u64>().unwrap_or(1) + 1;
        stream.version_id = next_version.to_string();
        let arn = stream.arn.clone();

        let mut attributes = BTreeMap::new();
        attributes.insert("Arn".to_string(), arn);
        attributes.insert("DeliveryStreamName".to_string(), name.clone());
        Ok(ProvisionResult {
            physical_id: name,
            attributes,
        })
    }

    pub(super) fn delete_firehose_delivery_stream(&self, physical_id: &str) -> Result<(), String> {
        let mut state = self.firehose_state.write();
        let account = state.get_or_create(&self.account_id, &self.region);
        account.streams_mut(&self.region).remove(physical_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{make_provisioner, make_resource};
    use super::*;
    use serde_json::{json, Value};

    fn configuration() -> Value {
        json!({
            "RoleARN": "arn:aws:iam::123456789012:role/fh",
            "BucketARN": "arn:aws-cn:s3:::bucket",
            "Prefix": "static/",
            "ErrorOutputPrefix": "errors/",
            "CompressionFormat": "ZIP",
            "ProcessingConfiguration": {"Enabled": true, "Processors": [{"Type": "Lambda"}]},
            "DataFormatConversionConfiguration": {"Enabled": true, "SchemaConfiguration": {"DatabaseName": "db"}},
            "CloudWatchLoggingOptions": {"Enabled": true, "LogGroupName": "group"},
            "CustomTimeZone": "America/New_York",
            "S3BackupMode": "Enabled",
            "FileExtension": ".data",
            "DynamicPartitioningConfiguration": {"Enabled": true, "RetryOptions": {"DurationInSeconds": 60}},
            "EncryptionConfiguration": {"KMSEncryptionConfig": {"AWSKMSKeyARN": "key"}},
            "S3BackupConfiguration": {"RoleARN": "backup-role", "BucketARN": "arn:aws:s3:::backup"}
        })
    }

    fn stored(prov: &ResourceProvisioner) -> DeliveryStream {
        prov.firehose_state
            .read()
            .get("123456789012")
            .unwrap()
            .streams("us-east-1")
            .unwrap()["events"]
            .clone()
    }

    #[test]
    fn firehose_shared_parser_retains_extended_fields_and_defaults() {
        let config = configuration();
        let dest = parse_firehose_s3_destination(&config).unwrap();
        assert_eq!(dest.buffering_size_mb, Some(5));
        assert_eq!(dest.buffering_interval_seconds, Some(300));
        assert_eq!(dest.prefix.as_deref(), Some("static/"));
        assert_eq!(dest.error_output_prefix.as_deref(), Some("errors/"));
        assert_eq!(dest.compression_format.as_deref(), Some("ZIP"));
        assert_eq!(dest.custom_time_zone.as_deref(), Some("America/New_York"));
        assert_eq!(dest.s3_backup_mode.as_deref(), Some("Enabled"));
        assert_eq!(dest.file_extension.as_deref(), Some(".data"));
        for (stored, field) in [
            (dest.processing_configuration, "ProcessingConfiguration"),
            (
                dest.data_format_conversion_configuration,
                "DataFormatConversionConfiguration",
            ),
            (dest.cloudwatch_logging_options, "CloudWatchLoggingOptions"),
            (
                dest.dynamic_partitioning_configuration,
                "DynamicPartitioningConfiguration",
            ),
            (dest.encryption_configuration, "EncryptionConfiguration"),
            (dest.s3_backup_description, "S3BackupConfiguration"),
        ] {
            assert_eq!(stored.as_ref(), Some(&config[field]), "{field}");
        }
        for bad in [
            json!({"BufferingHints": {"SizeInMBs": 1}}),
            json!({"BufferingHints": {"SizeInMBs": "5", "IntervalInSeconds": 300}}),
            json!({"BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 901}}),
            json!({"FileExtension": "invalid"}),
        ] {
            let mut value = config.clone();
            value
                .as_object_mut()
                .unwrap()
                .extend(bad.as_object().unwrap().clone());
            assert!(parse_firehose_s3_destination(&value).is_err(), "{value}");
        }
        assert!(
            parse_firehose_s3_destination(&json!({"BucketARN": "arn:aws:s3:::bucket"})).is_err()
        );
        assert!(parse_firehose_s3_destination(&json!({"RoleARN": "role"})).is_err());
    }

    #[test]
    fn firehose_create_update_retains_configuration_and_stable_identity() {
        for shape in [
            "S3DestinationConfiguration",
            "ExtendedS3DestinationConfiguration",
        ] {
            let prov = make_provisioner();
            let mut props = json!({"DeliveryStreamName": "events", "Tags": [{"Key": "env", "Value": "before"}]});
            props[shape] = configuration();
            let resource = make_resource(
                "AWS::KinesisFirehose::DeliveryStream",
                "Firehose",
                props.clone(),
            );
            let existing = prov.create_resource(&resource).unwrap();
            let before = stored(&prov);
            let original_dest = before.destination.as_ref().unwrap();
            assert_eq!(original_dest.file_extension.as_deref(), Some(".data"));
            assert_eq!(original_dest.compression_format.as_deref(), Some("ZIP"));
            assert_eq!(
                original_dest.processing_configuration.as_ref().unwrap()["Enabled"],
                true
            );
            assert_eq!(before.version_id, "1");
            props[shape]["Prefix"] = json!("after/");
            props[shape]["CompressionFormat"] = json!("GZIP");
            props[shape]["FileExtension"] = json!(".updated");
            props[shape]["BufferingHints"] = json!({"SizeInMBs": 128, "IntervalInSeconds": 1});
            props["Tags"][0]["Value"] = json!("after");
            let updated = prov
                .update_resource(
                    &existing,
                    &make_resource("AWS::KinesisFirehose::DeliveryStream", "Firehose", props),
                )
                .unwrap()
                .unwrap();
            let after = stored(&prov);
            let dest = after.destination.as_ref().unwrap();
            assert_eq!(after.instance_id, before.instance_id);
            assert_eq!(after.created_at, before.created_at);
            assert_eq!(after.arn, before.arn);
            assert_eq!(updated.physical_id, existing.physical_id);
            assert_eq!(dest.destination_id, original_dest.destination_id);
            assert_eq!(dest.prefix.as_deref(), Some("after/"));
            assert_eq!(dest.file_extension.as_deref(), Some(".updated"));
            assert_eq!(dest.compression_format.as_deref(), Some("GZIP"));
            assert_eq!(
                dest.processing_configuration,
                original_dest.processing_configuration
            );
            assert_eq!(dest.buffering_size_mb, Some(128));
            assert_eq!(dest.buffering_interval_seconds, Some(1));
            assert_eq!(after.version_id, "2");
            assert_eq!(after.tags.get("env").map(String::as_str), Some("after"));
        }
    }

    #[test]
    fn firehose_invalid_create_update_is_atomic() {
        let prov = make_provisioner();
        let props = json!({"DeliveryStreamName": "events", "ExtendedS3DestinationConfiguration": configuration(), "Tags": [{"Key": "env", "Value": "before"}]});
        let existing = prov
            .create_resource(&make_resource(
                "AWS::KinesisFirehose::DeliveryStream",
                "Firehose",
                props.clone(),
            ))
            .unwrap();
        let before = stored(&prov);
        for bad in [
            json!({"FileExtension": "bad"}),
            json!({"FileExtension": ".JSON"}),
            json!({"FileExtension": 1}),
            json!({"BufferingHints": {"SizeInMBs": 1}}),
            json!({"BufferingHints": {"SizeInMBs": 0, "IntervalInSeconds": 300}}),
            json!({"BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": "300"}}),
        ] {
            // Same FileExtension rules as CreateDeliveryStream/UpdateDestination,
            // reported against the offending member.
            let expected = if bad.get("FileExtension").is_some() {
                "FileExtension"
            } else {
                "BufferingHints"
            };
            let mut invalid = props.clone();
            invalid["ExtendedS3DestinationConfiguration"]
                .as_object_mut()
                .unwrap()
                .extend(bad.as_object().unwrap().clone());
            invalid["ExtendedS3DestinationConfiguration"]["Prefix"] = json!("changed/");
            invalid["Tags"][0]["Value"] = json!("changed");
            let resource = make_resource(
                "AWS::KinesisFirehose::DeliveryStream",
                "Firehose",
                invalid.clone(),
            );
            let error = prov
                .update_resource(&existing, &resource)
                .expect_err("invalid destination must fail the update");
            assert!(error.contains(expected), "{error}");
            let after = stored(&prov);
            assert_eq!(after.instance_id, before.instance_id);
            assert_eq!(
                serde_json::to_value(after).unwrap(),
                serde_json::to_value(&before).unwrap()
            );
            invalid["DeliveryStreamName"] = json!("invalid-create");
            let error = prov
                .create_resource(&make_resource(
                    "AWS::KinesisFirehose::DeliveryStream",
                    "BadFirehose",
                    invalid,
                ))
                .expect_err("invalid destination must fail the resource");
            assert!(error.contains(expected), "{error}");
            assert!(!prov
                .firehose_state
                .read()
                .get("123456789012")
                .unwrap()
                .streams("us-east-1")
                .unwrap()
                .contains_key("invalid-create"));
        }
    }

    #[tokio::test]
    async fn firehose_delete_recreate_invalidates_queued_and_captured_delivery() {
        use fakecloud_firehose::writer::{BatchWriter, DeliveryBatch, S3BatchWriter};
        use fakecloud_firehose::{delivery_config, FirehoseEngine, StreamKey};
        use fakecloud_s3::delivery::DeliveryWriteOutcome;

        let prov = make_provisioner();
        prov.s3_state
            .write()
            .get_or_create("123456789012")
            .buckets
            .insert(
                "bucket".into(),
                fakecloud_s3::S3Bucket::new("bucket", "us-east-1", "owner"),
            );
        let resource = make_resource(
            "AWS::KinesisFirehose::DeliveryStream",
            "Firehose",
            json!({
                "DeliveryStreamName": "events",
                "ExtendedS3DestinationConfiguration": {
                    "RoleARN": "role", "BucketARN": "arn:aws:s3:::bucket",
                    "BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 0}
                }
            }),
        );
        prov.create_resource(&resource).unwrap();
        let before = stored(&prov);
        let engine = Arc::new(FirehoseEngine::new(prov.firehose_state.clone()));
        engine.set_s3(prov.s3_state.clone());
        engine
            .enqueue(
                "123456789012",
                "us-east-1",
                "events",
                vec![bytes::Bytes::from_static(b"old")],
            )
            .unwrap();
        let batch = DeliveryBatch {
            stream: StreamKey {
                account_id: "123456789012".into(),
                region: "us-east-1".into(),
                name: "events".into(),
                instance_id: before.instance_id,
            },
            version_id: "1".into(),
            destination: delivery_config(before.destination.as_ref().unwrap()).unwrap(),
            records: vec![bytes::Bytes::from_static(b"captured-old")],
            oldest_arrival: Utc::now(),
            delivery_time: Utc::now(),
            object_key: "stale".into(),
        };
        prov.delete_firehose_delivery_stream("events").unwrap();
        let writer = S3BatchWriter::new(
            prov.firehose_state.clone(),
            prov.s3_state.clone(),
            Arc::new(parking_lot::RwLock::new(None)),
        );
        assert_eq!(
            writer.write(&batch).unwrap(),
            DeliveryWriteOutcome::Cancelled
        );
        prov.create_resource(&resource).unwrap();
        assert_ne!(stored(&prov).instance_id, before.instance_id);
        assert_eq!(
            writer.write(&batch).unwrap(),
            DeliveryWriteOutcome::Cancelled
        );
        engine
            .enqueue(
                "123456789012",
                "us-east-1",
                "events",
                vec![bytes::Bytes::from_static(b"new")],
            )
            .unwrap();
        assert_eq!(
            engine.shutdown().await,
            fakecloud_firehose::DrainReport {
                delivered_batches: 1,
                undelivered_records: 0,
                undelivered_bytes: 0,
            }
        );
        let state = prov.s3_state.read();
        let objects = &state.get("123456789012").unwrap().buckets["bucket"].objects;
        assert_eq!(objects.len(), 1);
        let fakecloud_persistence::BodyRef::Memory(body) = &objects.values().next().unwrap().body
        else {
            panic!("memory fixture")
        };
        assert_eq!(body.as_ref(), b"new");
    }
}
#[cfg(test)]
mod compression_tests {
    use super::super::tests::{make_provisioner, make_resource};
    use super::*;
    use serde_json::{json, Value};

    fn firehose_resource(base: &str) -> ResourceDefinition {
        let mut props = json!({
            "DeliveryStreamName": "cfn-compression",
            "Tags": [{"Key": "env", "Value": "before"}]
        });
        props[base] = json!({
            "RoleARN": "arn:aws:iam::123456789012:role/fh",
            "BucketARN": "arn:aws:s3:::bucket", "Prefix": "before/"
        });
        make_resource("AWS::KinesisFirehose::DeliveryStream", "Stream", props)
    }

    fn firehose_state(prov: &ResourceProvisioner) -> Value {
        serde_json::to_value(&*prov.firehose_state.read()).unwrap()
    }

    #[test]
    fn firehose_compression_invalid_create_is_resource_failure() {
        for base in [
            "S3DestinationConfiguration",
            "ExtendedS3DestinationConfiguration",
        ] {
            for bad in [
                json!("BROTLI"),
                json!("SNAPPY"),
                json!(null),
                json!(123),
                json!({}),
            ] {
                let prov = make_provisioner();
                let before = firehose_state(&prov);
                let mut resource = firehose_resource(base);
                resource.properties[base]["CompressionFormat"] = bad;
                let err = prov
                    .create_resource(&resource)
                    .expect_err("invalid enum must fail provisioning");
                assert!(err.contains("CompressionFormat"), "{err}");
                assert_eq!(firehose_state(&prov), before);
            }
        }
    }

    #[test]
    fn firehose_compression_invalid_update_preserves_original_stream() {
        for base in [
            "S3DestinationConfiguration",
            "ExtendedS3DestinationConfiguration",
        ] {
            let prov = make_provisioner();
            let resource = firehose_resource(base);
            let existing = prov.create_resource(&resource).unwrap();
            let before = firehose_state(&prov);
            for bad in [json!("BROTLI"), json!("gzip"), json!(false)] {
                let mut changed = firehose_resource(base);
                changed.properties[base]["Prefix"] = json!("changed/");
                changed.properties[base]["CompressionFormat"] = bad;
                changed.properties["Tags"] = json!([{ "Key": "env", "Value": "changed" }]);
                assert!(prov.update_resource(&existing, &changed).is_err());
                assert_eq!(firehose_state(&prov), before);
            }
        }
    }

    #[test]
    fn firehose_compression_invalid_backup_update_preserves_original_stream() {
        let prov = make_provisioner();
        let base = "ExtendedS3DestinationConfiguration";
        let resource = firehose_resource(base);
        let existing = prov.create_resource(&resource).unwrap();
        let before = firehose_state(&prov);
        let mut changed = firehose_resource(base);
        changed.properties[base]["Prefix"] = json!("changed/");
        changed.properties[base]["S3BackupConfiguration"] = json!({"CompressionFormat": 42});
        assert!(prov.update_resource(&existing, &changed).is_err());
        assert_eq!(firehose_state(&prov), before);
    }

    #[test]
    fn firehose_compression_invalid_backup_create_is_resource_failure() {
        let prov = make_provisioner();
        let mut resource = firehose_resource("ExtendedS3DestinationConfiguration");
        resource.properties["ExtendedS3DestinationConfiguration"]["S3BackupConfiguration"] =
            json!({"CompressionFormat": "BROTLI"});
        assert!(prov.create_resource(&resource).is_err());
        assert!(prov.firehose_state.read().accounts.is_empty());
    }

    #[test]
    fn firehose_compression_valid_values_and_defaults_provision() {
        for base in [
            "S3DestinationConfiguration",
            "ExtendedS3DestinationConfiguration",
        ] {
            for format in [
                None,
                Some("UNCOMPRESSED"),
                Some("GZIP"),
                Some("ZIP"),
                Some("Snappy"),
                Some("HADOOP_SNAPPY"),
            ] {
                let prov = make_provisioner();
                let mut resource = firehose_resource(base);
                if let Some(format) = format {
                    resource.properties[base]["CompressionFormat"] = json!(format);
                }
                let existing = prov.create_resource(&resource).unwrap();
                prov.update_resource(&existing, &resource)
                    .unwrap()
                    .expect("in-place update");
                let state = prov.firehose_state.read();
                let stream = &state
                    .get("123456789012")
                    .unwrap()
                    .streams("us-east-1")
                    .unwrap()["cfn-compression"];
                assert_eq!(
                    stream
                        .destination
                        .as_ref()
                        .unwrap()
                        .compression_format
                        .as_deref(),
                    format
                );
                assert_eq!(stream.version_id, "2");
            }
        }
    }
}
