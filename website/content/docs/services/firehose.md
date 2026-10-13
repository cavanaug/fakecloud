+++
title = "Firehose"
description = "Amazon Data Firehose delivery streams and immediate S3 delivery with uncompressed, GZIP, or Xerial Snappy output, JSON 1.1 protocol."
weight = 32
+++

fakecloud implements Amazon Data Firehose's JSON 1.1 control plane and real delivery to [S3](/docs/services/s3/) with uncompressed, GZIP, or Xerial Snappy output. `PutRecord` writes one object; `PutRecordBatch` concatenates the valid records in that request into one object. Separate calls do not share a buffer.

**Status: control-plane parity and immediate S3 delivery with uncompressed, GZIP, or Xerial Snappy output. Size/time buffering is not implemented.**

## Supported today

- **Delivery streams** — `CreateDeliveryStream` / `DescribeDeliveryStream` / `ListDeliveryStreams` / `DeleteDeliveryStream` / `UpdateDestination`. Both `DirectPut` and `KinesisStreamAsSource` source types round-trip; Kinesis-source polling is not implemented. Created streams are immediately `ACTIVE` and deletion removes them immediately.
- **Destinations** — `ExtendedS3DestinationConfiguration`, `RedshiftDestinationConfiguration`, `ElasticsearchDestinationConfiguration`, `AmazonopensearchserviceDestinationConfiguration`, `SplunkDestinationConfiguration`, `HttpEndpointDestinationConfiguration`, `SnowflakeDestinationConfiguration`, `IcebergDestinationConfiguration`. Configuration round-trips verbatim through `UpdateDestination`.
- **Buffering hints** — `BufferingHints` (`SizeInMBs`, `IntervalInSeconds`) are **range-checked** on `CreateDeliveryStream` and `UpdateDestination`:
  - `SizeInMBs`: 1 - 128 MB.
  - `IntervalInSeconds`: `0` or 60 - 900 s.

  Out-of-range values return `InvalidArgumentException`. These hints are configuration-only: delivery is immediate regardless of their values.
- **Records** — `PutRecord` / `PutRecordBatch` assign per-record `RecordId`s and deliver valid decoded data to a configured S3 destination. Binary bytes and producer-supplied delimiters are preserved exactly before compression; no separators are added unless the `AppendDelimiterToRecord` processor is enabled. Without it, an all-empty delivery produces no object.
- **New line delimiter** — when `ProcessingConfiguration` has `Enabled: true` and its `Processors` include `AppendDelimiterToRecord`, a newline (`\n`) is written after every record, including the last one and empty records, before any compression. This applies to API deliveries and to internal CloudWatch Logs and SES deliveries. The optional `Delimiter` parameter is stored but not interpreted: AWS documents only a new line delimiter. Other processors in the same configuration are not run. CloudFormation-created streams do not yet store `ProcessingConfiguration`, so this applies to streams configured through the Firehose API.
- **S3 output** — static prefixes, UTC date hierarchy, stream/version/time/UUID object names, and `FileExtension` overrides. Objects have `Content-Type: application/octet-stream` and the stored body's MD5 as their `ETag`. GZIP output has `Content-Encoding: gzip`; Snappy output has `Content-Encoding: snappy-java`; uncompressed output has no content encoding. Delivered bytes and metadata survive a restart in persistent storage mode, including records routed through the internal Logs/SES adapter.
- **Tags** — `ListTagsForDeliveryStream` / `TagDeliveryStream` / `UntagDeliveryStream`. Keyed by stream ARN.
- **Server-side encryption** — `StartDeliveryStreamEncryption` / `StopDeliveryStreamEncryption`. Start sets the stream's `DeliveryStreamEncryptionConfiguration` to `Status=ENABLED` with the requested `KeyType` (defaults `AWS_OWNED_CMK`; stores the `KeyARN` for `CUSTOMER_MANAGED_CMK`); Stop flips it to `DISABLED`. The config is surfaced on `DescribeDeliveryStream`. Unknown streams return `ResourceNotFoundException`.

## S3 object bytes and names

Create the destination bucket before sending records. Within one `PutRecordBatch`, records `abc`, `def\n`, and the binary bytes `00 ff` become exactly `abcdef\n` followed by `00 ff`. For compressed output, those concatenated bytes are encoded in one GZIP or Xerial Snappy stream. Producers must supply their own delimiters, such as a newline for each JSON record, or enable `AppendDelimiterToRecord`, which writes the same records as exactly `abc\ndef\n\n` followed by `00 ff 0a` before any compression.

For a static prefix, the key format is:

```text
<prefix>yyyy/MM/dd/HH/<stream>-<version>-yyyy-MM-dd-HH-mm-ss-<uuid><extension>
```

The prefix and suffix times are UTC. With immediate delivery they use the same delivery time. The version comes from the delivery stream configuration and increments on `UpdateDestination`. The UUID is a full hyphenated UUID.

An absent or empty prefix starts at `yyyy/MM/dd/HH/`. A prefix of `events/` produces `events/yyyy/MM/dd/HH/`; `events` produces `eventsyyyy/MM/dd/HH/`, without an inserted slash.

| `CompressionFormat` | Default extension | `Content-Encoding` |
| --- | --- | --- |
| `UNCOMPRESSED` (also the default when omitted) | None | None |
| `GZIP` | `.gz` | `gzip` |
| `Snappy` | `.snappy` | `snappy-java` |

Uncompressed output has no default extension. `FileExtension: ".custom.log"` replaces the default extension with `.custom.log`; an absent or empty override keeps the codec's default. Naming an object `.log` does not disable compression. `FileExtension` must be empty or a dot followed by lowercase letters, digits, or `!-_.*'()`, with at most 128 characters; other values return `InvalidArgumentException` from `CreateDeliveryStream` and `UpdateDestination`, and make a CloudFormation `AWS::KinesisFirehose::DeliveryStream` resource fail. `Content-Length` is the encoded object's byte length. S3 reads return the stored compressed bytes; decompress them to recover the original record bytes.

Use the exact enum spelling `Snappy` in `CompressionFormat`. Its output uses the Xerial/snappy-java container: a 16-byte version header followed by length-prefixed raw Snappy blocks, each covering up to 32 KiB of input. Record boundaries do not restart the block window. Read it with an Xerial-aware decoder; this is distinct from the standard Snappy framed-stream format and Hadoop-Snappy.

## Smoke test

```sh
fakecloud &

aws --endpoint-url http://localhost:4566 s3api create-bucket --bucket my-bucket

aws --endpoint-url http://localhost:4566 firehose create-delivery-stream \
  --delivery-stream-name events \
  --delivery-stream-type DirectPut \
  --extended-s3-destination-configuration '{
    "RoleARN": "arn:aws:iam::000000000000:role/firehose",
    "BucketARN": "arn:aws:s3:::my-bucket",
    "BufferingHints": {"SizeInMBs": 5, "IntervalInSeconds": 300},
    "CompressionFormat": "UNCOMPRESSED",
    "Prefix": "events/",
    "FileExtension": ".json"
  }'

aws --endpoint-url http://localhost:4566 firehose describe-delivery-stream \
  --delivery-stream-name events

aws --endpoint-url http://localhost:4566 firehose put-record \
  --delivery-stream-name events \
  --record Data=$(echo -n '{"id":"abc"}' | base64)

# Delivery is immediate even with the buffering hints above.
aws --endpoint-url http://localhost:4566 s3api list-objects-v2 --bucket my-bucket
# Read the returned key with s3api get-object to inspect the original bytes.
```

## Introspection

`GET /_fakecloud/firehose/delivery-streams` is an IAM-bypass admin endpoint that returns every delivery stream across all accounts and regions, so tests can assert stream state without round-tripping through `DescribeDeliveryStream` (and without credentials):

```sh
curl -fsS http://localhost:4566/_fakecloud/firehose/delivery-streams | jq
```

Response shape:

```json
{
  "deliveryStreams": [
    {
      "accountId": "123456789012",
      "name": "fh-intro",
      "arn": "arn:aws:firehose:us-east-1:123456789012:deliverystream/fh-intro",
      "streamType": "DirectPut",
      "status": "ACTIVE",
      "encryption": { "status": "ENABLED", "keyType": "AWS_OWNED_CMK" },
      "destinationCount": 1,
      "createTimestamp": "2026-05-29T00:00:00+00:00",
      "lastUpdateTimestamp": "2026-05-29T00:00:00+00:00"
    }
  ]
}
```

`encryption` is derived from the persisted SSE config: `{ "status": "DISABLED" }` when no encryption is configured, or `{ "status": "ENABLED", "keyType": ..., "keyArn": ... }` after `StartDeliveryStreamEncryption` (the `keyArn` is present only for customer-managed keys). Streams are sorted by account, then name.

All first-party SDKs ship a `firehose` sub-client wrapping this endpoint (`getDeliveryStreams()`):

- Rust: `fakecloud_sdk::FakeCloud::new(url).firehose().get_delivery_streams()`
- Go: `fakecloud.New(url).Firehose().GetDeliveryStreams(ctx)`
- Python: `await fc.firehose.get_delivery_streams()` (async) or `fc.firehose.get_delivery_streams()` (sync)
- TypeScript: `await fc.firehose.getDeliveryStreams()`
- Java: `fc.firehose().getDeliveryStreams()`
- PHP: `$fc->firehose()->getDeliveryStreams()`

See [`reference/introspection`](/docs/reference/introspection/) for the full endpoint catalog.

## Caveats

Only `UNCOMPRESSED`, `GZIP`, and `Snappy` S3 delivery are implemented. Other `CompressionFormat` values still round-trip as configuration, but selecting an unsupported codec does not encode the body or add a codec extension. Use one of the implemented formats when testing delivered objects.

Buffering hints do not aggregate separate calls or delay delivery. `Prefix` is used verbatim: `!{namespace:value}` expressions are not evaluated, so they appear literally in keys and the default `yyyy/MM/dd/HH/` hierarchy is still appended. Keys always use UTC; `CustomTimeZone` is stored but not applied. Processing configuration other than `AppendDelimiterToRecord` (Lambda and the other built-in processors), format conversion (Parquet/ORC), dynamic partitioning, delivered-object encryption, and S3 backup pipelines are configuration-only: records are written as received. Other destinations are not delivered to, and Firehose S3 writes do not dispatch S3 notifications.

An acknowledgment does not guarantee delivery: a missing destination bucket or malformed bucket ARN is logged while the API still returns success. These failed deliveries are not retained or retried. If a persistent-store write fails, the object is kept in memory for the current session only. Model-level record and batch size limits are not enforced.

## Source

- [`crates/fakecloud-firehose`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-firehose)
- [Amazon Data Firehose API reference](https://docs.aws.amazon.com/firehose/latest/APIReference/Welcome.html)
