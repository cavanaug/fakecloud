//! E2E coverage for the I1 SES introspection endpoints:
//! `/_fakecloud/ses/bounces`, `/_fakecloud/ses/messages/{id}/insights`,
//! `/_fakecloud/ses/smtp/submissions`,
//! `/_fakecloud/ses/event-destinations/deliveries`.
//!
//! Each test triggers the AWS operation that produces the backing state
//! (SendBounce / SendEmail / SMTP submission / event-destination dispatch
//! via a configured config-set) and then reads back the introspection
//! endpoint via HTTP, asserting the shape.

mod helpers;

use std::net::TcpListener as StdTcpListener;
use std::time::Duration;

use aws_sdk_iam::config::Region;
use aws_sdk_sesv2::types::{
    Body, Content, Destination, EmailContent, EventDestinationDefinition, EventType,
    KinesisFirehoseDestination, Message,
};
use helpers::TestServer;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

fn pick_free_port() -> u16 {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

async fn read_line(rd: &mut BufReader<tokio::net::tcp::OwnedReadHalf>) -> String {
    let mut buf = String::new();
    rd.read_line(&mut buf).await.expect("smtp read");
    buf
}

async fn wait_for_smtp(port: u16) -> TcpStream {
    for _ in 0..50 {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", port)).await {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("SMTP listener never came up on port {port}");
}

#[tokio::test]
async fn ses_bounces_introspection_returns_sendbounce_details() {
    use aws_sdk_ses::types::{BounceType, BouncedRecipientInfo, RecipientDsnFields};
    let server = TestServer::start().await;
    let endpoint = server.endpoint();

    let client = server.ses_client().await;
    let original = "0000000000000000-original@example.com";
    let bounced = BouncedRecipientInfo::builder()
        .recipient("user@example.com")
        .bounce_type(BounceType::DoesNotExist)
        .recipient_dsn_fields(
            RecipientDsnFields::builder()
                .status("5.1.1")
                .action(aws_sdk_ses::types::DsnAction::Failed)
                .diagnostic_code("smtp; 550 5.1.1 user unknown")
                .build()
                .expect("dsn"),
        )
        .build()
        .expect("bounced recipient");
    client
        .send_bounce()
        .original_message_id(original)
        .bounce_sender("mailer-daemon@example.com")
        .explanation("mailbox unavailable")
        .bounced_recipient_info_list(bounced)
        .send()
        .await
        .expect("send bounce");
    let http = reqwest::Client::new();

    let intro: Value = http
        .get(format!("{endpoint}/_fakecloud/ses/bounces"))
        .send()
        .await
        .expect("get bounces")
        .json()
        .await
        .expect("json");
    let bounces = intro["bounces"].as_array().expect("bounces array");
    assert_eq!(bounces.len(), 1, "{intro}");
    let b = &bounces[0];
    assert_eq!(b["originalMessageId"], original);
    assert_eq!(b["bounceSender"], "mailer-daemon@example.com");
    assert_eq!(b["explanation"], "mailbox unavailable");
    assert_eq!(b["bounceType"], "DoesNotExist");
    let infos = b["bouncedRecipientInfo"].as_array().unwrap();
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0]["recipient"], "user@example.com");
    assert_eq!(infos[0]["bounceType"], "DoesNotExist");
    assert_eq!(infos[0]["status"], "5.1.1");
    assert_eq!(infos[0]["diagnosticCode"], "smtp; 550 5.1.1 user unknown");
}

#[tokio::test]
async fn ses_message_insights_returns_per_recipient_events() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    client
        .create_email_identity()
        .email_identity("sender@example.com")
        .send()
        .await
        .expect("create identity");

    // success simulator => Send + Delivery events; bounce simulator =>
    // Send + Bounce. Send one of each so the insights endpoint has
    // populated `deliveries` and `bounces` arrays.
    let send_one = async |to: &str| {
        client
            .send_email()
            .from_email_address("sender@example.com")
            .destination(Destination::builder().to_addresses(to).build())
            .content(
                EmailContent::builder()
                    .simple(
                        Message::builder()
                            .subject(Content::builder().data("hi").build().unwrap())
                            .body(
                                Body::builder()
                                    .text(Content::builder().data("hi").build().unwrap())
                                    .build(),
                            )
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .map(|r| r.message_id().unwrap_or_default().to_string())
            .expect("send")
    };

    let success_id = send_one("success@simulator.amazonses.com").await;
    let bounce_id = send_one("bounce@simulator.amazonses.com").await;

    let http = reqwest::Client::new();
    let endpoint = server.endpoint();
    let success_insights: Value = http
        .get(format!(
            "{endpoint}/_fakecloud/ses/messages/{success_id}/insights"
        ))
        .send()
        .await
        .expect("get success insights")
        .json()
        .await
        .expect("json");
    assert_eq!(success_insights["messageId"], success_id);
    assert!(!success_insights["sends"].as_array().unwrap().is_empty());
    assert!(!success_insights["deliveries"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(success_insights["bounces"].as_array().unwrap().is_empty());

    let bounce_insights: Value = http
        .get(format!(
            "{endpoint}/_fakecloud/ses/messages/{bounce_id}/insights"
        ))
        .send()
        .await
        .expect("get bounce insights")
        .json()
        .await
        .expect("json");
    let bounces = bounce_insights["bounces"].as_array().unwrap();
    assert!(!bounces.is_empty(), "{bounce_insights}");
    assert_eq!(bounces[0]["destination"], "bounce@simulator.amazonses.com");

    // Unknown message id => 404.
    let resp = http
        .get(format!(
            "{endpoint}/_fakecloud/ses/messages/does-not-exist/insights"
        ))
        .send()
        .await
        .expect("missing msg");
    assert_eq!(resp.status().as_u16(), 404);
}

#[tokio::test]
async fn ses_smtp_submissions_introspection_reflects_smtp_listener() {
    let smtp_port = pick_free_port();
    let port_str = smtp_port.to_string();
    let server = TestServer::start_with_env(&[("FAKECLOUD_SES_SMTP_PORT", &port_str)]).await;

    // Provision IAM user + SES SMTP credential.
    let iam_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(Region::new("us-east-1"))
        .credentials_provider(aws_credential_types::Credentials::new(
            "test", "test", None, None, "test",
        ))
        .load()
        .await;
    let iam = aws_sdk_iam::Client::new(&iam_config);
    iam.create_user()
        .user_name("intro-smtp")
        .send()
        .await
        .expect("create user");
    let cred = iam
        .create_service_specific_credential()
        .user_name("intro-smtp")
        .service_name("ses.amazonaws.com")
        .send()
        .await
        .expect("create cred");
    let cred = cred.service_specific_credential().expect("cred body");
    let user = cred.service_user_name().to_string();
    let pass = cred.service_password().to_string();

    // Send one message over SMTP.
    let stream = wait_for_smtp(smtp_port).await;
    let (rd, mut wr) = stream.into_split();
    let mut rd = BufReader::new(rd);
    let _banner = read_line(&mut rd).await;
    wr.write_all(b"EHLO test\r\n").await.unwrap();
    loop {
        let line = read_line(&mut rd).await;
        if line.starts_with("250 ") {
            break;
        }
    }
    let blob = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        format!("\0{user}\0{pass}").as_bytes(),
    );
    wr.write_all(format!("AUTH PLAIN {blob}\r\n").as_bytes())
        .await
        .unwrap();
    let _ = read_line(&mut rd).await;
    wr.write_all(b"MAIL FROM:<src@example.com>\r\n")
        .await
        .unwrap();
    let _ = read_line(&mut rd).await;
    wr.write_all(b"RCPT TO:<dst@example.com>\r\n")
        .await
        .unwrap();
    let _ = read_line(&mut rd).await;
    wr.write_all(b"DATA\r\n").await.unwrap();
    let _ = read_line(&mut rd).await;
    wr.write_all(
        b"Subject: smtp-intro\r\nFrom: src@example.com\r\nTo: dst@example.com\r\n\r\nhello\r\n.\r\n",
    )
    .await
    .unwrap();
    let _ = read_line(&mut rd).await;
    wr.write_all(b"QUIT\r\n").await.unwrap();
    let _ = read_line(&mut rd).await;
    let mut sink = Vec::new();
    let _ = tokio::time::timeout(Duration::from_millis(50), rd.read_to_end(&mut sink)).await;

    let http = reqwest::Client::new();
    let intro: Value = http
        .get(format!(
            "{}/_fakecloud/ses/smtp/submissions",
            server.endpoint()
        ))
        .send()
        .await
        .expect("get submissions")
        .json()
        .await
        .expect("json");
    let subs = intro["submissions"].as_array().expect("submissions array");
    let sub = subs
        .iter()
        .find(|s| s["from"] == "src@example.com")
        .unwrap_or_else(|| panic!("missing SMTP submission: {intro}"));
    assert_eq!(sub["to"][0], "dst@example.com");
    assert_eq!(sub["subject"], "smtp-intro");
    assert_eq!(sub["authUser"], user);
    assert!(sub["rawSizeBytes"].as_u64().unwrap() > 0);
    assert!(sub["receivedAt"].as_str().unwrap().starts_with("20"));
}

#[tokio::test]
async fn ses_event_destination_deliveries_introspection_records_fanout() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;
    let endpoint = server.endpoint();
    server
        .s3_client()
        .await
        .create_bucket()
        .bucket("intro-ses-events")
        .send()
        .await
        .unwrap();
    let stream = server
        .firehose_client()
        .await
        .create_delivery_stream()
        .delivery_stream_name("intro-ses-events")
        .extended_s3_destination_configuration(
            aws_sdk_firehose::types::ExtendedS3DestinationConfiguration::builder()
                .bucket_arn("arn:aws:s3:::intro-ses-events")
                .role_arn("arn:aws:iam::123456789012:role/firehose")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let stream_arn = stream.delivery_stream_arn().unwrap();

    // Verify identity.
    client
        .create_email_identity()
        .email_identity("from@example.com")
        .send()
        .await
        .expect("create identity");

    // Create config set + Firehose event destination matching SEND + DELIVERY.
    client
        .create_configuration_set()
        .configuration_set_name("cs1")
        .send()
        .await
        .expect("create config set");
    client
        .create_configuration_set_event_destination()
        .configuration_set_name("cs1")
        .event_destination_name("fh")
        .event_destination(
            EventDestinationDefinition::builder()
                .enabled(true)
                .matching_event_types(EventType::Send)
                .matching_event_types(EventType::Delivery)
                .kinesis_firehose_destination(
                    KinesisFirehoseDestination::builder()
                        .iam_role_arn("arn:aws:iam::123456789012:role/firehose")
                        .delivery_stream_arn(stream_arn)
                        .build()
                        .expect("kf"),
                )
                .build(),
        )
        .send()
        .await
        .expect("create event destination");

    // Send an email pinned to the config set; success simulator yields
    // SEND + DELIVERY events.
    let sent = client
        .send_email()
        .from_email_address("from@example.com")
        .destination(
            Destination::builder()
                .to_addresses("success@simulator.amazonses.com")
                .build(),
        )
        .configuration_set_name("cs1")
        .content(
            EmailContent::builder()
                .simple(
                    Message::builder()
                        .subject(Content::builder().data("hi").build().unwrap())
                        .body(
                            Body::builder()
                                .text(Content::builder().data("hi").build().unwrap())
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("send email");

    let http = reqwest::Client::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let intro: Value = loop {
        let intro: Value = http
            .get(format!(
                "{endpoint}/_fakecloud/ses/event-destinations/deliveries"
            ))
            .send()
            .await
            .expect("get deliveries")
            .json()
            .await
            .expect("json");
        if intro["deliveries"].as_array().is_some_and(|d| d.len() >= 2) {
            break intro;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "expected SEND+DELIVERY dispatches: {intro}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    let deliveries = intro["deliveries"].as_array().expect("deliveries array");
    assert!(
        deliveries.len() >= 2,
        "expected SEND+DELIVERY dispatches: {intro}"
    );
    let event_types: Vec<&str> = deliveries
        .iter()
        .map(|d| d["eventType"].as_str().unwrap_or(""))
        .collect();
    assert!(event_types.contains(&"SEND"), "{event_types:?}");
    assert!(event_types.contains(&"DELIVERY"), "{event_types:?}");
    for d in deliveries {
        assert_eq!(d["destinationName"], "fh");
        assert_eq!(d["destinationType"], "firehose");
        assert_eq!(d["targetArn"], stream_arn);
        assert_eq!(d["messageId"], sent.message_id().unwrap());
    }
}
