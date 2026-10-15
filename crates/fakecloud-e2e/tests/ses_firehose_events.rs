//! SES event publishing through the official SDKs and the real Firehose -> S3 path.
//! Uses zero-interval, uncompressed delivery; SES supplies NDJSON framing.

use aws_sdk_firehose::types::{
    BufferingHints, CompressionFormat, ExtendedS3DestinationConfiguration,
};
use fakecloud_testkit::TestServer;
use serde_json::{json, Value};
use std::time::Duration;

const ROLE: &str = "arn:aws:iam::123456789012:role/firehose";

#[tokio::test]
async fn sesv2_sdk_raw_optional_fields_and_group_comments_deliver_via_domain_default() {
    use aws_sdk_sesv2::{
        primitives::Blob,
        types::{
            Destination, EmailContent, EventDestinationDefinition, EventType,
            KinesisFirehoseDestination, MailType, RawMessage,
        },
    };
    let server = TestServer::start().await;
    let arn = event_stream(&server, "ses-optional-raw").await;
    let ses = server.sesv2_client().await;
    ses.create_configuration_set()
        .configuration_set_name("events")
        .send()
        .await
        .unwrap();
    ses.create_configuration_set_event_destination()
        .configuration_set_name("events")
        .event_destination_name("firehose")
        .event_destination(
            EventDestinationDefinition::builder()
                .enabled(true)
                .matching_event_types(EventType::Send)
                .matching_event_types(EventType::Delivery)
                .kinesis_firehose_destination(
                    KinesisFirehoseDestination::builder()
                        .delivery_stream_arn(&arn)
                        .iam_role_arn(ROLE)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    ses.create_email_identity()
        .email_identity("example.com")
        .send()
        .await
        .unwrap();
    ses.put_email_identity_configuration_set_attributes()
        .email_identity("example.com")
        .configuration_set_name("events")
        .send()
        .await
        .unwrap();
    ses.put_account_details()
        .mail_type(MailType::Transactional)
        .website_url("https://example.com")
        .production_access_enabled(false)
        .send()
        .await
        .unwrap();
    let content = |text: &str| {
        EmailContent::builder()
            .raw(RawMessage::builder().data(Blob::new(text)).build().unwrap())
            .build()
    };
    // From, recipients and configuration set all come from MIME / identity
    // metadata, with none of the optional SendEmail parameters supplied.
    let raw = "From: \"user@department\"@example.com\r\nTo: Team: Chris <c@(host)example.com>, good@example.com; (team)\r\nCc: Empty: (nobody); (end)\r\nSubject: raw\r\nMIME-Version: 1.0\r\nContent-Type: text/plain\r\n\r\nbody";
    let header_only = ses.send_email().content(content(raw)).send().await.unwrap();
    let header_id = header_only.message_id().unwrap();
    // Explicit API envelope fields take precedence over other MIME addresses.
    let overridden = ses.send_email().from_email_address("API <sender@example.com>")
        .destination(Destination::builder().to_addresses("api@example.com").build())
        .configuration_set_name("events")
        .content(content("From: unverified@other.test\r\nTo: other@other.test\r\nX-SES-CONFIGURATION-SET: other-set\r\nMIME-Version: 1.0\r\nContent-Type: text/plain\r\n\r\nbody"))
        .send().await.unwrap();
    let override_id = overridden.message_id().unwrap();
    for invalid in [
        "From: sender@example.com\r\nMIME-Version: 1.0\r\nContent-Type: text/plain\r\n\r\nbody",
        "To: good@example.com\r\nMIME-Version: 1.0\r\nContent-Type: text/plain\r\n\r\nbody",
        "From: sender@example.com\r\nTo: good@example.com, not-an-address (label)\r\n\r\nbody",
    ] {
        assert!(ses
            .send_email()
            .content(content(invalid))
            .send()
            .await
            .is_err());
    }
    let events = delivered_events(&server, "ses-optional-raw", 4).await;
    for event in &events {
        assert_eq!(
            event["mail"]["tags"]["ses:configuration-set"],
            json!(["events"])
        );
        assert_eq!(
            event["mail"]["sourceArn"],
            "arn:aws:ses:us-east-1:123456789012:identity/example.com"
        );
        if event["mail"]["messageId"] == header_id {
            assert_eq!(event["mail"]["source"], r#""user@department"@example.com"#);
            assert_eq!(
                event["mail"]["destination"],
                json!(["c@example.com", "good@example.com"])
            );
        } else {
            assert_eq!(event["mail"]["messageId"], override_id);
            assert_eq!(event["mail"]["source"], "sender@example.com");
            assert_eq!(event["mail"]["destination"], json!(["api@example.com"]));
        }
    }
    assert_eq!(
        events
            .iter()
            .filter(|e| e["mail"]["messageId"] == header_id)
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| e["mail"]["messageId"] == override_id)
            .count(),
        2
    );
    let stored: Value = reqwest::get(format!("{}/_fakecloud/ses/emails", server.endpoint()))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        stored["emails"].as_array().unwrap().len(),
        2,
        "failed sends must not be stored: {stored}"
    );
}

#[tokio::test]
async fn ses_sdk_admission_rejects_bad_recipients_and_preserves_quoted_local_parts() {
    use aws_sdk_sesv2::types::{
        Body, BulkEmailContent, BulkEmailEntry, Content, Destination, EmailContent,
        EmailTemplateContent, EventDestinationDefinition, EventType, KinesisFirehoseDestination,
        MailType, Message, Template,
    };
    let server = TestServer::start().await;
    let arn = event_stream(&server, "ses-admission-events").await;
    let v2 = server.sesv2_client().await;
    let v1 = server.ses_client().await;
    v2.create_email_identity()
        .email_identity("example.com")
        .send()
        .await
        .unwrap();
    v2.put_account_details()
        .mail_type(MailType::Transactional)
        .website_url("https://example.com")
        .production_access_enabled(false)
        .send()
        .await
        .unwrap();
    v2.create_configuration_set()
        .configuration_set_name("events")
        .send()
        .await
        .unwrap();
    v2.create_configuration_set_event_destination()
        .configuration_set_name("events")
        .event_destination_name("firehose")
        .event_destination(
            EventDestinationDefinition::builder()
                .enabled(true)
                .matching_event_types(EventType::Send)
                .matching_event_types(EventType::Delivery)
                .kinesis_firehose_destination(
                    KinesisFirehoseDestination::builder()
                        .delivery_stream_arn(&arn)
                        .iam_role_arn(ROLE)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    v2.create_email_template()
        .template_name("admission")
        .template_content(
            EmailTemplateContent::builder()
                .subject("subject")
                .text("body")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let content = EmailContent::builder()
        .simple(
            Message::builder()
                .subject(Content::builder().data("subject").build().unwrap())
                .body(
                    Body::builder()
                        .text(Content::builder().data("body").build().unwrap())
                        .build(),
                )
                .build(),
        )
        .build();
    let default_content = BulkEmailContent::builder()
        .template(
            Template::builder()
                .template_name("admission")
                .template_data("{}")
                .build(),
        )
        .build();
    let group = "Team: Unverified <one@other.test>, Verified <two@example.com>;";
    assert!(v2
        .send_email()
        .from_email_address("sender@example.com")
        .configuration_set_name("events")
        .destination(Destination::builder().to_addresses(group).build())
        .content(content.clone())
        .send()
        .await
        .is_err());
    let rejected = v2
        .send_bulk_email()
        .from_email_address("sender@example.com")
        .configuration_set_name("events")
        .default_content(default_content.clone())
        .bulk_email_entries(
            BulkEmailEntry::builder()
                .destination(Destination::builder().to_addresses(group).build())
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        rejected.bulk_email_entry_results()[0]
            .status()
            .unwrap()
            .as_str(),
        "INVALID_PARAMETER_VALUE"
    );
    assert!(rejected.bulk_email_entry_results()[0]
        .message_id()
        .is_none());
    for to in [
        "not-an-address",
        "good@example.com, not-an-address",
        "Empty:;",
    ] {
        let raw = format!(
            "From: sender@example.com\r\nTo: {to}\r\nX-SES-CONFIGURATION-SET: events\r\n\r\nbody"
        );
        assert!(v1
            .send_raw_email()
            .raw_message(
                aws_sdk_ses::types::RawMessage::builder()
                    .data(aws_sdk_ses::primitives::Blob::new(raw))
                    .build()
                    .unwrap()
            )
            .send()
            .await
            .is_err());
    }
    assert!(server
        .s3_client()
        .await
        .list_objects_v2()
        .bucket("ses-admission-events")
        .send()
        .await
        .unwrap()
        .contents()
        .is_empty());
    let emails: Value = reqwest::get(format!("{}/_fakecloud/ses/emails", server.endpoint()))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(emails["emails"].as_array().unwrap().is_empty(), "{emails}");

    let from = r#"Sender <"s,t"@example.com>"#;
    let bare_from = r#""s,t"@example.com"#;
    let to = r#"Recipient <"a,b"@example.com>"#;
    let bare_to = r#""a,b"@example.com"#;
    let mut ids = Vec::new();
    ids.push(
        v2.send_email()
            .from_email_address(from)
            .configuration_set_name("events")
            .destination(Destination::builder().to_addresses(bare_to).build())
            .content(content)
            .send()
            .await
            .unwrap()
            .message_id()
            .unwrap()
            .to_string(),
    );
    let bulk = v2
        .send_bulk_email()
        .from_email_address(bare_from)
        .configuration_set_name("events")
        .default_content(default_content)
        .bulk_email_entries(
            BulkEmailEntry::builder()
                .destination(Destination::builder().to_addresses(to).build())
                .build(),
        )
        .send()
        .await
        .unwrap();
    ids.push(
        bulk.bulk_email_entry_results()[0]
            .message_id()
            .unwrap()
            .to_string(),
    );
    for address in [bare_to, to] {
        let raw = format!("From: {bare_from}\r\nTo: Empty:;, Team: {address};\r\nX-SES-CONFIGURATION-SET: events\r\n\r\nbody");
        ids.push(
            v1.send_raw_email()
                .raw_message(
                    aws_sdk_ses::types::RawMessage::builder()
                        .data(aws_sdk_ses::primitives::Blob::new(raw))
                        .build()
                        .unwrap(),
                )
                .send()
                .await
                .unwrap()
                .message_id()
                .to_string(),
        );
    }
    ids.push(
        v1.send_email()
            .source(from)
            .configuration_set_name("events")
            .destination(
                aws_sdk_ses::types::Destination::builder()
                    .to_addresses(to)
                    .build(),
            )
            .message(
                aws_sdk_ses::types::Message::builder()
                    .subject(
                        aws_sdk_ses::types::Content::builder()
                            .data("subject")
                            .build()
                            .unwrap(),
                    )
                    .body(
                        aws_sdk_ses::types::Body::builder()
                            .text(
                                aws_sdk_ses::types::Content::builder()
                                    .data("body")
                                    .build()
                                    .unwrap(),
                            )
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap()
            .message_id()
            .to_string(),
    );
    let events = delivered_events(&server, "ses-admission-events", ids.len() * 2).await;
    for id in ids {
        let matching: Vec<_> = events
            .iter()
            .filter(|e| e["mail"]["messageId"] == id)
            .collect();
        assert_eq!(matching.len(), 2);
        for event in matching {
            assert_eq!(event["mail"]["source"], bare_from);
            assert_eq!(event["mail"]["destination"], json!([bare_to]));
            if event["eventType"] == "Delivery" {
                assert_eq!(event["delivery"]["recipients"], json!([bare_to]));
            }
        }
    }
}

async fn event_stream(server: &TestServer, bucket: &str) -> String {
    server
        .s3_client()
        .await
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    server
        .firehose_client()
        .await
        .create_delivery_stream()
        .delivery_stream_name("ses-events")
        .extended_s3_destination_configuration(
            ExtendedS3DestinationConfiguration::builder()
                .bucket_arn(format!("arn:aws:s3:::{bucket}"))
                .role_arn(ROLE)
                .compression_format(CompressionFormat::Uncompressed)
                .buffering_hints(
                    BufferingHints::builder()
                        .size_in_mbs(1)
                        .interval_in_seconds(0)
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap()
        .delivery_stream_arn()
        .unwrap()
        .to_string()
}

async fn delivered_events(server: &TestServer, bucket: &str, expected: usize) -> Vec<Value> {
    let s3 = server.s3_client().await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let listing = s3.list_objects_v2().bucket(bucket).send().await.unwrap();
        let mut events = Vec::new();
        for object in listing.contents() {
            let object = s3
                .get_object()
                .bucket(bucket)
                .key(object.key().unwrap())
                .send()
                .await
                .unwrap();
            let bytes = object.body.collect().await.unwrap().into_bytes();
            assert_eq!(
                bytes.last(),
                Some(&b'\n'),
                "SES must delimit each JSON record"
            );
            let lines = std::str::from_utf8(&bytes).expect("uncompressed UTF-8 NDJSON");
            for line in lines.lines() {
                assert!(!line.is_empty(), "exactly one newline per record");
                events.push(serde_json::from_str(line).unwrap());
            }
        }
        if events.len() >= expected {
            assert_eq!(
                events.len(),
                expected,
                "unexpected duplicate or failed-send events"
            );
            return events;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "expected {expected} events, found {}: {events:?}",
            events.len()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn assert_envelope(event: &Value, id: &str, destinations: Value) {
    assert_eq!(event["mail"]["messageId"], id);
    assert_eq!(event["mail"]["source"], "sender@example.com");
    assert_eq!(event["mail"]["destination"], destinations);
    assert_eq!(event["mail"]["sendingAccountId"], "123456789012");
    assert_eq!(
        event["mail"]["sourceArn"],
        "arn:aws:ses:us-east-1:123456789012:identity/example.com"
    );
    assert_eq!(event["mail"]["headersTruncated"], false);
    assert_eq!(
        event["mail"]["tags"]["ses:configuration-set"],
        json!(["events"])
    );
    assert!(event["mail"]["timestamp"].as_str().is_some());
}

#[tokio::test]
async fn sesv2_sdk_firehose_destination_publishes_send_and_delivery_to_s3() {
    use aws_sdk_sesv2::types::{
        Body, Content, Destination, EmailContent, EventDestinationDefinition, EventType,
        KinesisFirehoseDestination, Message, MessageTag,
    };
    let server = TestServer::start().await;
    let arn = event_stream(&server, "sesv2-events").await;
    let ses = server.sesv2_client().await;
    ses.create_email_identity()
        .email_identity("example.com")
        .send()
        .await
        .unwrap();
    ses.create_configuration_set()
        .configuration_set_name("events")
        .send()
        .await
        .unwrap();
    ses.create_configuration_set_event_destination()
        .configuration_set_name("events")
        .event_destination_name("firehose")
        .event_destination(
            EventDestinationDefinition::builder()
                .enabled(true)
                .matching_event_types(EventType::Send)
                .matching_event_types(EventType::Delivery)
                .kinesis_firehose_destination(
                    KinesisFirehoseDestination::builder()
                        .delivery_stream_arn(&arn)
                        .iam_role_arn(ROLE)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    let destinations = ses
        .get_configuration_set_event_destinations()
        .configuration_set_name("events")
        .send()
        .await
        .unwrap();
    let dest = &destinations.event_destinations()[0];
    assert_eq!(
        dest.kinesis_firehose_destination()
            .unwrap()
            .delivery_stream_arn(),
        arn
    );
    assert_eq!(
        dest.kinesis_firehose_destination().unwrap().iam_role_arn(),
        ROLE
    );
    assert_eq!(
        dest.matching_event_types(),
        &[EventType::Send, EventType::Delivery]
    );
    let output = ses
        .send_email()
        .from_email_address("Sender <sender@example.com>")
        .configuration_set_name("events")
        .destination(
            Destination::builder()
                .to_addresses("Recipient <to@example.com>")
                .cc_addresses("Copy <cc@example.com>")
                .bcc_addresses("bcc@example.com")
                .build(),
        )
        .email_tags(
            MessageTag::builder()
                .name("campaign")
                .value("fall")
                .build()
                .unwrap(),
        )
        .content(
            EmailContent::builder()
                .simple(
                    Message::builder()
                        .subject(Content::builder().data("event publishing").build().unwrap())
                        .body(
                            Body::builder()
                                .text(Content::builder().data("body").build().unwrap())
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    let id = output.message_id().unwrap();
    let events = delivered_events(&server, "sesv2-events", 2).await;
    for event in &events {
        assert_envelope(
            event,
            id,
            json!(["to@example.com", "cc@example.com", "bcc@example.com"]),
        );
        assert_eq!(event["mail"]["tags"]["campaign"], json!(["fall"]));
    }
    assert!(events
        .iter()
        .any(|e| e["eventType"] == "Send" && e["send"] == json!({})));
    assert!(events.iter().any(|e| e["eventType"] == "Delivery"
        && e["delivery"]["recipients"]
            == json!(["to@example.com", "cc@example.com", "bcc@example.com"])));
}

#[tokio::test]
async fn sesv1_sdk_sends_and_raw_header_config_publish_to_firehose_s3() {
    use aws_sdk_ses::{
        primitives::Blob,
        types::{
            Body, BulkEmailDestination, ConfigurationSet, Content, Destination, EventDestination,
            EventType, KinesisFirehoseDestination, Message, RawMessage, Template,
        },
    };
    let server = TestServer::start().await;
    let arn = event_stream(&server, "sesv1-events").await;
    let ses = server.ses_client().await;
    ses.verify_domain_identity()
        .domain("example.com")
        .send()
        .await
        .unwrap();
    server
        .sesv2_client()
        .await
        .put_account_details()
        .mail_type(aws_sdk_sesv2::types::MailType::Transactional)
        .website_url("https://example.com")
        .production_access_enabled(false)
        .send()
        .await
        .unwrap();
    ses.create_configuration_set()
        .configuration_set(ConfigurationSet::builder().name("events").build().unwrap())
        .send()
        .await
        .unwrap();
    ses.create_configuration_set_event_destination()
        .configuration_set_name("events")
        .event_destination(
            EventDestination::builder()
                .name("firehose")
                .enabled(true)
                .matching_event_types(EventType::Send)
                .matching_event_types(EventType::Bounce)
                .kinesis_firehose_destination(
                    KinesisFirehoseDestination::builder()
                        .delivery_stream_arn(&arn)
                        .iam_role_arn(ROLE)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let config = ses
        .describe_configuration_set()
        .configuration_set_name("events")
        .configuration_set_attribute_names(
            aws_sdk_ses::types::ConfigurationSetAttribute::EventDestinations,
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        config.event_destinations()[0].matching_event_types(),
        &[EventType::Send, EventType::Bounce]
    );
    assert_eq!(
        config.event_destinations()[0]
            .kinesis_firehose_destination()
            .unwrap()
            .delivery_stream_arn(),
        arn
    );
    let output = ses
        .send_email()
        .source("Sender <sender@example.com>")
        .configuration_set_name("events")
        .destination(
            Destination::builder()
                .to_addresses("Bounce <bounce@simulator.amazonses.com>")
                .build(),
        )
        .message(
            Message::builder()
                .subject(Content::builder().data("bounce").build().unwrap())
                .body(
                    Body::builder()
                        .text(Content::builder().data("body").build().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    let bounce_id = output.message_id().to_string();
    let output = ses.send_raw_email().raw_message(RawMessage::builder().data(Blob::new(
        "From: sender@example.com (Sender)\r\nTo: to@example.com (Recipient), Team: one@example.com, two@example.com;\r\nCc: \"Copy, One\" <cc@example.com>\r\nBcc: bcc@example.com\r\nX-SES-CONFIGURATION-SET: events\r\nSubject: raw\r\n\r\nTo: body@other.test"
    )).build().unwrap()).send().await.unwrap();
    let raw_id = output.message_id().to_string();
    let output = ses
        .send_raw_email()
        .source("Sender <sender@example.com>")
        .destinations("Bounce <bounce@simulator.amazonses.com>")
        .configuration_set_name("events")
        .raw_message(
            RawMessage::builder()
                .data(Blob::new(
                    "From: sender@example.com\r\nTo: ignored@example.com\r\n\r\nbody",
                ))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let raw_api_id = output.message_id().to_string();
    let rejected = ses
        .send_email()
        .source("unverified@other.test")
        .configuration_set_name("events")
        .destination(
            Destination::builder()
                .to_addresses("to@example.com")
                .build(),
        )
        .message(
            Message::builder()
                .subject(Content::builder().data("rejected").build().unwrap())
                .body(
                    Body::builder()
                        .text(Content::builder().data("body").build().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await;
    assert!(rejected
        .unwrap_err()
        .as_service_error()
        .unwrap()
        .is_message_rejected());
    ses.create_template()
        .template(
            Template::builder()
                .template_name("tpl")
                .subject_part("Hi {{name}}")
                .text_part("Hello {{name}}")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let output = ses
        .send_templated_email()
        .source("Sender <sender@example.com>")
        .configuration_set_name("events")
        .destination(
            Destination::builder()
                .to_addresses("Recipient <to@example.com>")
                .build(),
        )
        .template("tpl")
        .template_data(r#"{"name":"Ada"}"#)
        .send()
        .await
        .unwrap();
    let template_id = output.message_id().to_string();
    let output = ses
        .send_bulk_templated_email()
        .source("Sender <sender@example.com>")
        .configuration_set_name("events")
        .template("tpl")
        .default_template_data(r#"{"name":"Ada"}"#)
        .destinations(
            BulkEmailDestination::builder()
                .destination(
                    Destination::builder()
                        .cc_addresses("Copy <cc@example.com>")
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    let bulk_id = output.status()[0].message_id().unwrap().to_string();
    let events = delivered_events(&server, "sesv1-events", 7).await;
    for (id, to, count) in [
        (&bounce_id, json!(["bounce@simulator.amazonses.com"]), 2),
        (
            &raw_id,
            json!([
                "to@example.com",
                "one@example.com",
                "two@example.com",
                "cc@example.com",
                "bcc@example.com"
            ]),
            1,
        ),
        (&raw_api_id, json!(["bounce@simulator.amazonses.com"]), 2),
        (&template_id, json!(["to@example.com"]), 1),
        (&bulk_id, json!(["cc@example.com"]), 1),
    ] {
        let matching: Vec<_> = events
            .iter()
            .filter(|e| e["mail"]["messageId"] == *id)
            .collect();
        assert_eq!(matching.len(), count, "message {id}");
        for event in matching {
            assert_envelope(event, id, to.clone());
        }
    }
    assert!(events.iter().any(|e| e["eventType"] == "Bounce"
        && e["bounce"]["bouncedRecipients"][0]["emailAddress"]
            == "bounce@simulator.amazonses.com"));
}

#[tokio::test]
async fn sesv2_mixed_simulators_partition_firehose_records_and_insights() {
    use aws_sdk_sesv2::types::{
        Body, Content, Destination, EmailContent, EventDestinationDefinition, EventType,
        KinesisFirehoseDestination, Message,
    };
    let server = TestServer::start().await;
    let arn = event_stream(&server, "ses-mixed-events").await;
    let ses = server.sesv2_client().await;
    ses.create_email_identity()
        .email_identity("example.com")
        .send()
        .await
        .unwrap();
    ses.create_configuration_set()
        .configuration_set_name("events")
        .send()
        .await
        .unwrap();
    ses.create_configuration_set_event_destination()
        .configuration_set_name("events")
        .event_destination_name("firehose")
        .event_destination(
            EventDestinationDefinition::builder()
                .enabled(true)
                .matching_event_types(EventType::Send)
                .matching_event_types(EventType::Delivery)
                .matching_event_types(EventType::Bounce)
                .matching_event_types(EventType::Complaint)
                .kinesis_firehose_destination(
                    KinesisFirehoseDestination::builder()
                        .delivery_stream_arn(&arn)
                        .iam_role_arn(ROLE)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    let output = ses
        .send_email()
        .from_email_address("Sender <sender@example.com>")
        .configuration_set_name("events")
        .destination(
            Destination::builder()
                .to_addresses("Normal <normal@example.com>")
                .cc_addresses("Bounce <bounce@simulator.amazonses.com>")
                .bcc_addresses("Complaint <complaint@simulator.amazonses.com>")
                .build(),
        )
        .content(
            EmailContent::builder()
                .simple(
                    Message::builder()
                        .subject(Content::builder().data("mixed outcomes").build().unwrap())
                        .body(
                            Body::builder()
                                .text(Content::builder().data("body").build().unwrap())
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    let id = output.message_id().unwrap();
    let events = delivered_events(&server, "ses-mixed-events", 4).await;
    assert_eq!(
        events.iter().filter(|e| e["eventType"] == "Send").count(),
        1
    );
    for event in &events {
        assert_envelope(
            event,
            id,
            json!([
                "normal@example.com",
                "bounce@simulator.amazonses.com",
                "complaint@simulator.amazonses.com"
            ]),
        );
    }
    let event = |kind: &str| events.iter().find(|e| e["eventType"] == kind).unwrap();
    assert_eq!(
        event("Delivery")["delivery"]["recipients"],
        json!(["normal@example.com", "complaint@simulator.amazonses.com"])
    );
    let bounce = event("Bounce")["bounce"]["bouncedRecipients"]
        .as_array()
        .unwrap();
    assert_eq!(bounce.len(), 1);
    assert_eq!(bounce[0]["emailAddress"], "bounce@simulator.amazonses.com");
    assert_eq!(
        event("Complaint")["complaint"]["complainedRecipients"],
        json!([{"emailAddress":"complaint@simulator.amazonses.com"}])
    );
    let insights: Value = reqwest::get(format!(
        "{}/_fakecloud/ses/messages/{id}/insights",
        server.endpoint()
    ))
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    let destinations = |kind: &str| {
        insights[kind]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["destination"].as_str().unwrap())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        destinations("sends"),
        [
            "normal@example.com",
            "bounce@simulator.amazonses.com",
            "complaint@simulator.amazonses.com"
        ]
    );
    assert_eq!(
        destinations("deliveries"),
        ["normal@example.com", "complaint@simulator.amazonses.com"]
    );
    assert_eq!(destinations("bounces"), ["bounce@simulator.amazonses.com"]);
    assert_eq!(
        destinations("complaints"),
        ["complaint@simulator.amazonses.com"]
    );
}
