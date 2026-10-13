use std::time::Duration;

use fakecloud_core::service::AwsServiceError;
use http::StatusCode;
use serde_json::Value;

use crate::output::{append_delimiter_enabled, Compression, OutputConfig};
use crate::state::S3Destination;

#[derive(Clone, Debug)]
pub struct BufferingPolicy {
    pub size_bytes: usize,
    pub interval: Duration,
}

#[derive(Clone, Debug)]
pub struct DeliveryConfig {
    pub bucket: String,
    pub output: OutputConfig,
    pub buffering: BufferingPolicy,
}

fn invalid_argument(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidArgumentException", message)
}

/// Validate the paired API hints and materialize Firehose's S3 defaults.
pub fn buffering_policy(
    size: Option<i64>,
    interval: Option<i64>,
) -> Result<BufferingPolicy, AwsServiceError> {
    let (size, interval) = match (size, interval) {
        (None, None) => (5, 300),
        (Some(size), Some(interval)) => (size, interval),
        _ => {
            return Err(invalid_argument(
                "BufferingHints must specify both SizeInMBs and IntervalInSeconds",
            ))
        }
    };
    if !(1..=128).contains(&size) {
        return Err(invalid_argument(format!(
            "BufferingHints.SizeInMBs must be between 1 and 128, got {size}"
        )));
    }
    if !(0..=900).contains(&interval) {
        return Err(invalid_argument(format!(
            "BufferingHints.IntervalInSeconds must be between 0 and 900, got {interval}"
        )));
    }
    Ok(BufferingPolicy {
        size_bytes: size as usize * 1_048_576,
        interval: Duration::from_secs(interval as u64),
    })
}

/// Read supplied integers strictly: a malformed member is not an omitted hint.
pub(crate) fn parse_buffering_hints(
    value: &Value,
) -> Result<(Option<i64>, Option<i64>), AwsServiceError> {
    let Some(hints) = value.get("BufferingHints") else {
        return Ok((None, None));
    };
    if !hints.is_object() {
        return Err(invalid_argument("BufferingHints must be an object"));
    }
    let integer = |field: &str| {
        hints
            .get(field)
            .map(|value| {
                value.as_i64().ok_or_else(|| {
                    invalid_argument(format!("BufferingHints.{field} must be an integer"))
                })
            })
            .transpose()
    };
    let size = integer("SizeInMBs")?;
    let interval = integer("IntervalInSeconds")?;
    buffering_policy(size, interval)?;
    Ok((size, interval))
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

/// Shared control-plane parser. Unsupported delivery features still round-trip;
/// record admission uses `delivery_config` separately.
pub fn parse_s3_destination(value: &Value) -> Result<Option<S3Destination>, AwsServiceError> {
    crate::service::parse_s3_destination(value)
}

fn unsupported(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_IMPLEMENTED, "NotImplemented", message)
}

/// Capture an owned, immutable delivery configuration before record admission.
/// This helper does not restrict configuration create/update/describe acceptance.
pub fn delivery_config(dest: &S3Destination) -> Result<DeliveryConfig, AwsServiceError> {
    let compression = match dest.compression_format.as_deref().unwrap_or("UNCOMPRESSED") {
        "UNCOMPRESSED" => Compression::Uncompressed,
        "GZIP" => Compression::Gzip,
        "Snappy" => Compression::Snappy,
        codec => {
            return Err(unsupported(format!(
                "S3 delivery compression {codec} is not implemented"
            )))
        }
    };
    let prefix = dest.prefix.as_deref().unwrap_or_default();
    if prefix.contains("!{") {
        return Err(unsupported(
            "S3 delivery prefix expressions are not implemented",
        ));
    }
    if !matches!(dest.custom_time_zone.as_deref(), None | Some("" | "UTC")) {
        return Err(unsupported(
            "S3 delivery in a non-UTC time zone is not implemented",
        ));
    }
    if let Some(message) = unsupported_processing(dest.processing_configuration.as_ref()) {
        return Err(unsupported(message));
    }
    if dest
        .data_format_conversion_configuration
        .as_ref()
        .is_some_and(|value| value["Enabled"].as_bool() != Some(false))
    {
        return Err(unsupported(
            "S3 delivery data format conversion is not implemented",
        ));
    }
    if dest
        .dynamic_partitioning_configuration
        .as_ref()
        .is_some_and(|value| value["Enabled"].as_bool() == Some(true))
    {
        return Err(unsupported(
            "S3 delivery dynamic partitioning is not implemented",
        ));
    }
    let bucket = fakecloud_aws::arn::arn_resource(&dest.bucket_arn, "s3")
        .and_then(|resource| resource.strip_prefix("::"))
        .filter(|bucket| !bucket.is_empty())
        .ok_or_else(|| invalid_argument("S3 destination requires a bucket ARN"))?;
    // Older snapshots can omit either hint; fill defaults independently here.
    let buffering = buffering_policy(
        Some(dest.buffering_size_mb.unwrap_or(5)),
        Some(dest.buffering_interval_seconds.unwrap_or(300)),
    )?;
    Ok(DeliveryConfig {
        bucket: bucket.to_owned(),
        output: OutputConfig {
            prefix: prefix.to_owned(),
            file_extension: dest.file_extension.clone(),
            compression,
            append_delimiter: append_delimiter_enabled(dest.processing_configuration.as_ref()),
        },
        buffering,
    })
}

/// Enabled processing is delivered only when every processor is the built-in
/// `AppendDelimiterToRecord`; anything else (Lambda, other processor types, or
/// no processors at all) would change record bytes in ways this emulator does
/// not run, so it is refused rather than silently skipped.
fn unsupported_processing(processing: Option<&Value>) -> Option<String> {
    let processing = processing.filter(|value| value["Enabled"].as_bool() == Some(true))?;
    let Some(processors) = processing["Processors"]
        .as_array()
        .filter(|processors| !processors.is_empty())
    else {
        return Some(
            "S3 delivery processing without an AppendDelimiterToRecord processor is not implemented"
                .to_owned(),
        );
    };
    processors
        .iter()
        .find(|processor| processor["Type"] != "AppendDelimiterToRecord")
        .map(|processor| {
            let kind = processor["Type"].as_str().unwrap_or("untyped");
            format!(
                "S3 delivery processing with a {kind} processor is not implemented; only AppendDelimiterToRecord is supported"
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::Compression;
    use http::StatusCode;
    use serde_json::json;
    use std::time::Duration;

    fn destination(extra: serde_json::Value) -> crate::S3Destination {
        let mut value = json!({"RoleARN": "arn:aws:iam::123456789012:role/fh", "BucketARN": "arn:aws:s3:::bucket"});
        value
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        parse_s3_destination(&value).unwrap().unwrap()
    }

    fn unsupported(dest: &crate::S3Destination) {
        let error = delivery_config(dest).unwrap_err();
        assert_eq!(error.status(), StatusCode::NOT_IMPLEMENTED);
        assert_eq!(error.code(), "NotImplemented");
    }

    #[test]
    fn default_buffering() {
        let policy = buffering_policy(None, None).unwrap();
        assert_eq!(policy.size_bytes, 5 * 1_048_576);
        assert_eq!(policy.interval, Duration::from_secs(300));
        let dest = destination(json!({}));
        assert_eq!(dest.buffering_size_mb, Some(5));
        assert_eq!(dest.buffering_interval_seconds, Some(300));
        let mut legacy = dest;
        legacy.buffering_size_mb = None;
        legacy.buffering_interval_seconds = None;
        let config = delivery_config(&legacy).unwrap();
        assert_eq!(config.buffering.size_bytes, 5 * 1_048_576);
        assert_eq!(config.buffering.interval, Duration::from_secs(300));
        legacy.buffering_size_mb = Some(1);
        assert_eq!(
            delivery_config(&legacy).unwrap().buffering.size_bytes,
            1_048_576
        );
    }

    #[test]
    fn paired_hints_and_interval_boundaries() {
        for (size, interval) in [(1, 0), (1, 1), (128, 900)] {
            let policy = buffering_policy(Some(size), Some(interval)).unwrap();
            assert_eq!(policy.size_bytes, size as usize * 1_048_576);
            assert_eq!(policy.interval, Duration::from_secs(interval as u64));
        }
        for (size, interval) in [
            (Some(0), Some(300)),
            (Some(129), Some(300)),
            (Some(1), Some(-1)),
            (Some(1), Some(901)),
            (Some(1), None),
            (None, Some(300)),
        ] {
            let error = buffering_policy(size, interval).unwrap_err();
            assert_eq!(error.code(), "InvalidArgumentException");
            assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        }
    }

    #[test]
    fn typed_integer_hints() {
        for bad in [
            json!("5"),
            json!(1.5),
            json!(true),
            json!(null),
            json!(u64::MAX),
            json!({}),
        ] {
            for field in ["SizeInMBs", "IntervalInSeconds"] {
                let mut value = json!({"RoleARN": "role", "BucketARN": "arn:aws:s3:::bucket", "BufferingHints": {"SizeInMBs": 5, "IntervalInSeconds": 300}});
                value["BufferingHints"][field] = bad.clone();
                assert_eq!(
                    parse_s3_destination(&value).unwrap_err().code(),
                    "InvalidArgumentException",
                    "{value}"
                );
            }
        }
        for hints in [
            json!(null),
            json!("hints"),
            json!([]),
            json!({"SizeInMBs": 5}),
            json!({"IntervalInSeconds": 300}),
        ] {
            let value = json!({"RoleARN": "role", "BucketARN": "arn:aws:s3:::bucket", "BufferingHints": hints});
            assert!(parse_s3_destination(&value).is_err(), "{value}");
        }
    }

    #[test]
    fn delivery_preflight() {
        for (codec, expected) in [
            ("UNCOMPRESSED", Compression::Uncompressed),
            ("GZIP", Compression::Gzip),
            ("Snappy", Compression::Snappy),
        ] {
            for zone in [None, Some(""), Some("UTC")] {
                for prefix in [None, Some("static/")] {
                    let mut dest = destination(json!({"CompressionFormat": codec}));
                    dest.custom_time_zone = zone.map(str::to_owned);
                    dest.prefix = prefix.map(str::to_owned);
                    let config = delivery_config(&dest).unwrap();
                    assert_eq!(config.bucket, "bucket");
                    assert_eq!(config.output.compression, expected);
                    assert_eq!(config.output.prefix, prefix.unwrap_or_default());
                    let captured = config.clone();
                    dest.prefix = Some("changed/".into());
                    assert_eq!(captured.output.prefix, prefix.unwrap_or_default());
                    assert!(!format!("{captured:?}").is_empty());
                }
            }
        }
        assert_eq!(
            delivery_config(&destination(json!({})))
                .unwrap()
                .output
                .compression,
            Compression::Uncompressed
        );
        for codec in ["ZIP", "HADOOP_SNAPPY", "SNAPPY"] {
            unsupported(&destination(json!({"CompressionFormat": codec})));
        }
        unsupported(&destination(json!({"Prefix": "!{timestamp:yyyy}/"})));
        unsupported(&destination(json!({"CustomTimeZone": "America/New_York"})));
        for partition in ["aws", "aws-cn", "aws-us-gov"] {
            let dest = destination(json!({"BucketARN": format!("arn:{partition}:s3:::bucket")}));
            assert_eq!(delivery_config(&dest).unwrap().bucket, "bucket");
        }
        assert!(delivery_config(&destination(json!({"BucketARN": "not-an-arn"}))).is_err());
    }

    #[test]
    fn enabled_pipeline_preflight() {
        for extra in [
            json!({"ProcessingConfiguration": {"Enabled": true}}),
            json!({"DataFormatConversionConfiguration": {"Enabled": true}}),
            json!({"DataFormatConversionConfiguration": {"SchemaConfiguration": {}}}),
            json!({"DynamicPartitioningConfiguration": {"Enabled": true}}),
        ] {
            unsupported(&destination(extra));
        }
        let config = delivery_config(&destination(json!({
            "ProcessingConfiguration": {"Enabled": false, "Processors": [{"Type": "Lambda"}]},
            "DataFormatConversionConfiguration": {"Enabled": false},
            "DynamicPartitioningConfiguration": {"Enabled": false}
        })))
        .unwrap();
        assert_eq!(config.bucket, "bucket");
        assert!(!config.output.append_delimiter);
    }

    #[test]
    fn only_append_delimiter_processing_is_delivered() {
        let delimiter = json!({"Type": "AppendDelimiterToRecord"});
        let with_parameter = json!({"Type": "AppendDelimiterToRecord", "Parameters": [{"ParameterName": "Delimiter", "ParameterValue": "\\n"}]});
        for (processing, append_delimiter) in [
            (json!({"Enabled": true, "Processors": [delimiter]}), true),
            (
                json!({"Enabled": true, "Processors": [with_parameter]}),
                true,
            ),
            (
                json!({"Enabled": true, "Processors": [delimiter, delimiter]}),
                true,
            ),
            (
                json!({"Enabled": false, "Processors": [{"Type": "Lambda"}]}),
                false,
            ),
            (json!({"Processors": [delimiter]}), false),
            (
                json!({"Enabled": "true", "Processors": [{"Type": "Lambda"}]}),
                false,
            ),
        ] {
            let dest = destination(json!({"ProcessingConfiguration": processing}));
            assert_eq!(
                delivery_config(&dest).unwrap().output.append_delimiter,
                append_delimiter,
                "{processing}"
            );
        }
        for (processing, reason) in [
            (
                json!({"Enabled": true}),
                "without an AppendDelimiterToRecord",
            ),
            (
                json!({"Enabled": true, "Processors": []}),
                "without an AppendDelimiterToRecord",
            ),
            (
                json!({"Enabled": true, "Processors": {"Type": "AppendDelimiterToRecord"}}),
                "without an AppendDelimiterToRecord",
            ),
            (
                json!({"Enabled": true, "Processors": [{"Type": "Lambda"}]}),
                "Lambda processor",
            ),
            (
                json!({"Enabled": true, "Processors": [{"Type": "Lambda"}, delimiter]}),
                "Lambda processor",
            ),
            (
                json!({"Enabled": true, "Processors": [delimiter, {"Type": "RecordDeAggregation"}]}),
                "RecordDeAggregation processor",
            ),
            (
                json!({"Enabled": true, "Processors": [delimiter, {}]}),
                "untyped processor",
            ),
        ] {
            let dest = destination(json!({"ProcessingConfiguration": processing}));
            unsupported(&dest);
            let message = delivery_config(&dest).unwrap_err().message();
            assert!(message.contains("processing"), "{message}");
            assert!(message.contains(reason), "{processing}: {message}");
        }
    }

    #[test]
    fn file_extension_validation() {
        for extension in ["", ".log", ".a0!-_.*'()"] {
            let dest = destination(json!({"FileExtension": extension}));
            assert_eq!(
                delivery_config(&dest)
                    .unwrap()
                    .output
                    .file_extension
                    .as_deref(),
                Some(extension)
            );
        }
        let valid = format!(".{}", "a".repeat(127));
        assert_eq!(
            destination(json!({"FileExtension": valid}))
                .file_extension
                .as_deref(),
            Some(valid.as_str())
        );
        for extension in [
            "log".to_string(),
            ".".into(),
            ".UPPER".into(),
            ".é".into(),
            ".a/b".into(),
            ".a b".into(),
            format!(".{}", "a".repeat(128)),
        ] {
            let value = json!({"RoleARN": "role", "BucketARN": "arn:aws:s3:::bucket", "FileExtension": extension});
            assert_eq!(
                parse_s3_destination(&value).unwrap_err().code(),
                "InvalidArgumentException"
            );
        }
        assert!(parse_s3_destination(
            &json!({"RoleARN": "role", "BucketARN": "arn:aws:s3:::bucket", "FileExtension": 1})
        )
        .is_err());
    }
}
