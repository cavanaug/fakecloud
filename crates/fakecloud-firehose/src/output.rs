use std::io::Write;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Compression {
    Uncompressed,
    Gzip,
}

impl Compression {
    pub(crate) fn content_encoding(self) -> Option<&'static str> {
        match self {
            Self::Uncompressed => None,
            Self::Gzip => Some("gzip"),
        }
    }

    fn extension(self) -> &'static str {
        match self {
            Self::Uncompressed => "",
            Self::Gzip => ".gz",
        }
    }
}

pub(crate) struct OutputConfig<'a> {
    pub prefix: &'a str,
    pub file_extension: Option<&'a str>,
    pub compression: Compression,
    /// The built-in `AppendDelimiterToRecord` processor: write `\n` after
    /// every record.
    pub append_delimiter: bool,
}

/// Whether an S3 destination's `ProcessingConfiguration` enables the built-in
/// `AppendDelimiterToRecord` processor. Only `Enabled: true` counts. AWS
/// documents only a new line delimiter, so a `Delimiter` parameter is not
/// interpreted. Other processor types are not run.
pub(crate) fn append_delimiter_enabled(processing_configuration: Option<&Value>) -> bool {
    processing_configuration.is_some_and(|processing| {
        processing["Enabled"].as_bool() == Some(true)
            && processing["Processors"]
                .as_array()
                .is_some_and(|processors| {
                    processors
                        .iter()
                        .any(|processor| processor["Type"] == "AppendDelimiterToRecord")
                })
    })
}

/// Firehose concatenates the raw record bytes, then compresses the whole
/// payload as one stream. Producers supply any record delimiters unless
/// `AppendDelimiterToRecord` adds `\n` after each record (before compression).
pub(crate) fn encode_records(
    records: &[Vec<u8>],
    config: &OutputConfig<'_>,
) -> std::io::Result<Bytes> {
    let delimiter: &[u8] = if config.append_delimiter { b"\n" } else { b"" };
    match config.compression {
        Compression::Uncompressed => {
            let mut payload = Vec::with_capacity(
                records
                    .iter()
                    .map(|record| record.len() + delimiter.len())
                    .sum(),
            );
            for record in records {
                payload.extend_from_slice(record);
                payload.extend_from_slice(delimiter);
            }
            Ok(Bytes::from(payload))
        }
        Compression::Gzip => {
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            for record in records {
                encoder.write_all(record)?;
                encoder.write_all(delimiter)?;
            }
            Ok(Bytes::from(encoder.finish()?))
        }
    }
}

/// S3 keys are logical names, not filesystem paths. Do not normalize the prefix
/// or insert a separator between it and the default UTC time hierarchy.
pub(crate) fn object_key(
    stream: &str,
    version: &str,
    config: &OutputConfig<'_>,
    oldest_arrival: DateTime<Utc>,
    delivery_time: DateTime<Utc>,
    uuid: Uuid,
) -> String {
    format!(
        "{}{}/{stream}-{version}-{}-{uuid}{}",
        config.prefix,
        oldest_arrival.format("%Y/%m/%d/%H"),
        delivery_time.format("%Y-%m-%d-%H-%M-%S"),
        config
            .file_extension
            .filter(|extension| !extension.is_empty())
            .unwrap_or_else(|| config.compression.extension()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use std::io::Read;
    use uuid::Uuid;

    fn time(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn config(compression: Compression, append_delimiter: bool) -> OutputConfig<'static> {
        OutputConfig {
            prefix: "",
            file_extension: None,
            compression,
            append_delimiter,
        }
    }

    fn gunzip(encoded: &[u8]) -> Vec<u8> {
        assert!(encoded.starts_with(&[0x1f, 0x8b, 8]));
        let mut decoded = Vec::new();
        flate2::read::GzDecoder::new(encoded)
            .read_to_end(&mut decoded)
            .unwrap();
        decoded
    }

    #[test]
    fn gzip_encodes_a_single_stream_of_verbatim_binary_records() {
        let binary: Vec<u8> = (0..70_000).map(|i| ((i * 31 + 7) % 256) as u8).collect();
        let encoded = encode_records(
            &[b"abc".to_vec(), b"def\n".to_vec(), binary.clone()],
            &config(Compression::Gzip, false),
        )
        .unwrap();
        let decoded = gunzip(&encoded);
        assert_eq!(&decoded[..7], b"abcdef\n");
        assert_eq!(&decoded[7..], binary);
    }

    #[test]
    fn gzip_extensions_replace_the_default() {
        let now = time("2026-10-12T09:00:00Z");
        let uuid = Uuid::parse_str("12345678-1234-4234-8234-123456789abc").unwrap();
        for (extension, suffix) in [
            (None, ".gz"),
            (Some(""), ".gz"),
            (Some(".custom.log"), ".custom.log"),
        ] {
            let config = OutputConfig {
                prefix: "",
                file_extension: extension,
                compression: Compression::Gzip,
                append_delimiter: false,
            };
            assert_eq!(object_key("events", "1", &config, now, now, uuid), format!(
                "2026/10/12/09/events-1-2026-10-12-09-00-00-12345678-1234-4234-8234-123456789abc{suffix}"));
        }
    }

    #[test]
    fn raw_bytes_are_verbatim() {
        assert_eq!(
            encode_records(
                &[b"abc".to_vec(), b"def\n".to_vec(), vec![0, 255]],
                &config(Compression::Uncompressed, false)
            )
            .unwrap()
            .as_ref(),
            b"abcdef\n\x00\xff"
        );
        assert!(
            encode_records(&[vec![], vec![]], &config(Compression::Uncompressed, false))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn append_delimiter_terminates_every_record() {
        let records = [b"a".to_vec(), b"b\n".to_vec(), vec![0, 255]];
        let expected = b"a\nb\n\n\x00\xff\n";
        let plain = config(Compression::Uncompressed, true);
        assert_eq!(encode_records(&records, &plain).unwrap().as_ref(), expected);
        assert_eq!(encode_records(&[vec![]], &plain).unwrap().as_ref(), b"\n");
        assert!(encode_records(&[], &plain).unwrap().is_empty());
        // The delimiter is part of the payload that GZIP compresses.
        let gzip = config(Compression::Gzip, true);
        assert_eq!(gunzip(&encode_records(&records, &gzip).unwrap()), expected);
    }

    #[test]
    fn append_delimiter_requires_enabled_processor() {
        use serde_json::json;

        for (processing, expected) in [
            (
                json!({"Enabled": true, "Processors": [{"Type": "AppendDelimiterToRecord"}]}),
                true,
            ),
            (
                json!({"Enabled": true, "Processors": [{"Type": "Lambda"}, {"Type": "AppendDelimiterToRecord", "Parameters": [{"ParameterName": "Delimiter", "ParameterValue": "\\n"}]}]}),
                true,
            ),
            (
                json!({"Enabled": false, "Processors": [{"Type": "AppendDelimiterToRecord"}]}),
                false,
            ),
            (
                json!({"Processors": [{"Type": "AppendDelimiterToRecord"}]}),
                false,
            ),
            (
                json!({"Enabled": "true", "Processors": [{"Type": "AppendDelimiterToRecord"}]}),
                false,
            ),
            (
                json!({"Enabled": true, "Processors": [{"Type": "Lambda"}]}),
                false,
            ),
            (
                json!({"Enabled": true, "Processors": {"Type": "AppendDelimiterToRecord"}}),
                false,
            ),
            (json!({"Enabled": true}), false),
        ] {
            assert_eq!(
                append_delimiter_enabled(Some(&processing)),
                expected,
                "{processing}"
            );
        }
        assert!(!append_delimiter_enabled(None));
    }

    #[test]
    fn static_prefix_and_suffix_use_distinct_utc_times() {
        let config = OutputConfig {
            prefix: "events/",
            file_extension: None,
            compression: Compression::Uncompressed,
            append_delimiter: false,
        };
        let key = object_key(
            "events",
            "2",
            &config,
            time("2026-10-12T09:59:59Z"),
            time("2026-10-12T10:00:02Z"),
            Uuid::parse_str("12345678-1234-4234-8234-123456789abc").unwrap(),
        );
        assert_eq!(key,
            "events/2026/10/12/09/events-2-2026-10-12-10-00-02-12345678-1234-4234-8234-123456789abc");
    }

    #[test]
    fn prefixes_and_extensions_are_appended_verbatim() {
        for (prefix, extension, expected) in [
            ("", None, "2026/10/12/09/events-1-2026-10-12-09-00-00-12345678-1234-4234-8234-123456789abc"),
            ("events", Some(""), "events2026/10/12/09/events-1-2026-10-12-09-00-00-12345678-1234-4234-8234-123456789abc"),
            ("events/", Some(".custom.log"), "events/2026/10/12/09/events-1-2026-10-12-09-00-00-12345678-1234-4234-8234-123456789abc.custom.log"),
        ] {
            let config = OutputConfig { prefix, file_extension: extension, compression: Compression::Uncompressed, append_delimiter: false };
            let now = time("2026-10-12T09:00:00Z");
            assert_eq!(object_key("events", "1", &config, now, now,
                Uuid::parse_str("12345678-1234-4234-8234-123456789abc").unwrap()), expected);
        }
    }
}
