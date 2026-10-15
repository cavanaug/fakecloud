//! SES event fanout: publishes send/delivery/bounce/complaint events
//! to configured event destinations (SNS topics, EventBridge buses).

use base64::Engine;
use chrono::Utc;
use serde_json::json;
use std::sync::Arc;

use fakecloud_core::delivery::DeliveryBus;

use crate::state::{
    DeliveryInsightEvent, EmailRecipientInsight, EventDestination, EventDestinationDispatch,
    SentEmail, SharedSesState, SuppressedDestination,
};

/// Shared references needed for cross-service event delivery. Deliberately
/// carries no account or region: every send is fanned out in the sending
/// account and region (see [`SendScope`]), never a server-wide default.
#[derive(Clone)]
pub struct SesDeliveryContext {
    pub ses_state: SharedSesState,
    pub delivery_bus: Arc<DeliveryBus>,
}

/// The request account used for SES state lookups and the region of emitted
/// events. SES storage is currently account-scoped; this scope keeps fanout
/// from substituting server-wide defaults for either request field.
#[derive(Debug, Clone, Copy)]
pub struct SendScope<'a> {
    pub account_id: &'a str,
    pub region: &'a str,
}

/// Mailbox simulator addresses.
const BOUNCE_ADDR: &str = "bounce@simulator.amazonses.com";
const COMPLAINT_ADDR: &str = "complaint@simulator.amazonses.com";
#[cfg(test)]
const SUCCESS_ADDR: &str = "success@simulator.amazonses.com";
const SUPPRESSION_ADDR: &str = "suppressionlist@simulator.amazonses.com";
const OOTO_ADDR: &str = "ooto@simulator.amazonses.com";
const SOFTBOUNCE_ADDR: &str = "softbounce@simulator.amazonses.com";
const TRANSIENT_BOUNCE_ADDR: &str = "transient-bounce@simulator.amazonses.com";

/// The event types we generate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SesEventType {
    Send,
    Delivery,
    Bounce,
    Complaint,
}

impl SesEventType {
    fn as_str(self) -> &'static str {
        match self {
            SesEventType::Send => "SEND",
            SesEventType::Delivery => "DELIVERY",
            SesEventType::Bounce => "BOUNCE",
            SesEventType::Complaint => "COMPLAINT",
        }
    }

    /// The EventBridge `detail-type` SES publishes this event under.
    fn eventbridge_detail_type(self) -> &'static str {
        match self {
            SesEventType::Send => "Email Sent",
            SesEventType::Delivery => "Email Delivered",
            SesEventType::Bounce => "Email Bounced",
            SesEventType::Complaint => "Email Complaint Received",
        }
    }

    fn event_type_name(self) -> &'static str {
        match self {
            SesEventType::Send => "Send",
            SesEventType::Delivery => "Delivery",
            SesEventType::Bounce => "Bounce",
            SesEventType::Complaint => "Complaint",
        }
    }
}

/// Build the SES event JSON payload matching the AWS notification format.
pub fn build_ses_event(event_type: SesEventType, email: &SentEmail) -> serde_json::Value {
    let recipients = envelope_recipients(email);
    build_ses_event_for_recipients(event_type, email, &recipients)
}

fn build_ses_event_for_recipients(
    event_type: SesEventType,
    email: &SentEmail,
    event_recipients: &[String],
) -> serde_json::Value {
    let recipients = envelope_recipients(email);
    let mut event = json!({
        "eventType": event_type.event_type_name(),
        "mail": {
            "messageId": email.message_id,
            "source": crate::mime::mailbox_address(&email.from).unwrap_or_default(),
            "destination": recipients,
            "timestamp": email.timestamp.to_rfc3339(),
        },
    });

    // Add event-type-specific detail blocks
    match event_type {
        SesEventType::Send => {
            event["send"] = json!({});
        }
        SesEventType::Delivery => {
            event["delivery"] = json!({
                "timestamp": Utc::now().to_rfc3339(),
                "recipients": event_recipients,
                "processingTimeMillis": 42,
                "smtpResponse": "250 2.0.0 Ok",
            });
        }
        SesEventType::Bounce => {
            let bounced: Vec<serde_json::Value> = event_recipients
                .iter()
                .map(|addr| {
                    json!({
                        "emailAddress": addr,
                        "action": "failed",
                        "status": "5.1.1",
                        "diagnosticCode": "smtp; 550 5.1.1 user unknown",
                    })
                })
                .collect();
            event["bounce"] = json!({
                "bounceType": "Permanent",
                "bounceSubType": "General",
                "bouncedRecipients": bounced,
                "timestamp": Utc::now().to_rfc3339(),
            });
        }
        SesEventType::Complaint => {
            let complained: Vec<serde_json::Value> = event_recipients
                .iter()
                .map(|addr| json!({ "emailAddress": addr }))
                .collect();
            event["complaint"] = json!({
                "complainedRecipients": complained,
                "complaintFeedbackType": "abuse",
                "timestamp": Utc::now().to_rfc3339(),
            });
        }
    }

    event
}

fn envelope_recipients(email: &SentEmail) -> Vec<String> {
    email
        .to
        .iter()
        .chain(&email.cc)
        .chain(&email.bcc)
        .filter_map(|value| crate::mime::mailbox_address(value))
        .collect()
}

/// The verified identity a send went out under: the From address when it is
/// itself a verified identity, otherwise its domain.
fn sending_identity(ses_state: &SharedSesState, account_id: &str, from: &str) -> String {
    let address = crate::mime::mailbox_address(from).unwrap_or_default();
    let domain = address
        .rsplit_once('@')
        .map_or(address.as_str(), |(_, d)| d);
    let mas = ses_state.read();
    let address_verified = mas
        .get(account_id)
        .is_some_and(|s| s.identities.contains_key(&address));
    if address_verified {
        address.to_string()
    } else {
        domain.to_string()
    }
}

/// Build the event published to configuration-set destinations: the base
/// event plus the `mail` fields AWS stamps on every published event
/// (`sendingAccountId`, `sourceArn`, `headersTruncated`, `tags` with the
/// `ses:` auto-tags and the message tags).
fn build_published_event(
    ctx: &SesDeliveryContext,
    scope: SendScope<'_>,
    event_type: SesEventType,
    email: &SentEmail,
    config_set: &str,
    event_recipients: &[String],
) -> serde_json::Value {
    let mut event = build_ses_event_for_recipients(event_type, email, event_recipients);
    let identity = sending_identity(&ctx.ses_state, scope.account_id, &email.from);
    let source_arn = fakecloud_aws::arn::Arn::regional(
        "ses",
        scope.region,
        scope.account_id,
        &format!("identity/{identity}"),
    )
    .to_string();
    let address = crate::mime::mailbox_address(&email.from).unwrap_or_default();
    let from_domain = address
        .rsplit_once('@')
        .map_or(address.as_str(), |(_, d)| d);
    let mut tags = serde_json::Map::new();
    tags.insert("ses:configuration-set".to_string(), json!([config_set]));
    tags.insert("ses:from-domain".to_string(), json!([from_domain]));
    tags.insert("ses:caller-identity".to_string(), json!([scope.account_id]));
    for (name, value) in &email.email_tags {
        tags.insert(name.clone(), json!([value]));
    }
    event["mail"]["sendingAccountId"] = json!(scope.account_id);
    event["mail"]["sourceArn"] = json!(source_arn);
    event["mail"]["headersTruncated"] = json!(false);
    event["mail"]["tags"] = serde_json::Value::Object(tags);
    event
}

/// Determine which event types to generate based on recipient addresses.
/// Returns the list of event types to emit and whether to add to suppression list.
pub fn classify_recipients(recipients: &[String]) -> (Vec<SesEventType>, bool) {
    let mut events = vec![SesEventType::Send];
    let mut suppress = false;
    for recipient in recipients
        .iter()
        .filter_map(|r| crate::mime::mailbox_address(r))
    {
        let (types, add_to_suppression) = recipient_event_types(&recipient);
        suppress |= add_to_suppression;
        for event in types {
            if !events.contains(event) {
                events.push(*event);
            }
        }
    }
    (events, suppress)
}

/// Simulator outcomes are per mailbox. A bounce must not mask delivery or a
/// complaint for another recipient of the same message.
fn recipient_event_types(recipient: &str) -> (&'static [SesEventType], bool) {
    use SesEventType::{Bounce, Complaint, Delivery, Send};
    match recipient {
        SUPPRESSION_ADDR => (&[Send, Bounce], true),
        BOUNCE_ADDR | SOFTBOUNCE_ADDR | TRANSIENT_BOUNCE_ADDR => (&[Send, Bounce], false),
        COMPLAINT_ADDR | OOTO_ADDR => (&[Send, Delivery, Complaint], false),
        // Normal, success and forwarding mailboxes all deliver.
        _ => (&[Send, Delivery], false),
    }
}

/// Check if any recipient is on the suppression list AND the stored
/// reason is enforced under the effective `SuppressedReasons` filter
/// (configuration-set scope first, then account-level fallback). Lookup
/// is case-insensitive. Returns the suppressed address if found.
pub fn check_suppression_list(
    ses_state: &SharedSesState,
    account_id: &str,
    recipients: &[String],
    config_set_name: Option<&str>,
) -> Option<String> {
    let mas = ses_state.read();
    let state = mas.get(account_id)?;
    for addr in recipients {
        if state.suppressed_match(addr, config_set_name).is_some() {
            return Some(addr.clone());
        }
    }
    None
}

/// Resolve the configuration set name for an email send.
/// Checks the explicit request param first, then the identity's default.
pub fn resolve_config_set(
    ses_state: &SharedSesState,
    account_id: &str,
    explicit_config_set: Option<&str>,
    from_address: &str,
) -> Option<String> {
    if let Some(name) = explicit_config_set {
        return Some(name.to_string());
    }

    // Check identity's default configuration set
    let from_address = crate::mime::mailbox_address(from_address)?;
    let mas = ses_state.read();
    let state = mas.get(account_id)?;
    if let Some(identity) = state.identities.get(&from_address) {
        return identity.configuration_set_name.clone();
    }
    // Also check domain identity
    if let Some((_, domain)) = from_address.rsplit_once('@') {
        if let Some(identity) = state.identities.get(domain) {
            return identity.configuration_set_name.clone();
        }
    }
    None
}

/// Get enabled event destinations for a configuration set that match the given event type.
fn get_matching_destinations(
    ses_state: &SharedSesState,
    account_id: &str,
    config_set_name: &str,
    event_type: &str,
) -> Vec<EventDestination> {
    let mas = ses_state.read();
    let Some(state) = mas.get(account_id) else {
        return Vec::new();
    };
    state
        .event_destinations
        .get(config_set_name)
        .map(|dests| {
            dests
                .iter()
                .filter(|d| {
                    d.enabled
                        && d.matching_event_types.iter().any(|t| {
                            // v1 uses lowercase/camelCase (renderingFailure), v2
                            // uppercase/snake_case (RENDERING_FAILURE). Normalize
                            // only while matching so API roundtrips retain spelling.
                            t.bytes()
                                .filter(|b| *b != b'_')
                                .map(|b| b.to_ascii_uppercase())
                                .eq(event_type
                                    .bytes()
                                    .filter(|b| *b != b'_')
                                    .map(|b| b.to_ascii_uppercase()))
                        })
                })
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

/// Fan out a single event to all matching destinations.
fn deliver_event(
    ctx: &SesDeliveryContext,
    scope: SendScope<'_>,
    event: &serde_json::Value,
    event_type: SesEventType,
    config_set_name: &str,
) {
    let destinations = get_matching_destinations(
        &ctx.ses_state,
        scope.account_id,
        config_set_name,
        event_type.as_str(),
    );

    let mut dispatches: Vec<EventDestinationDispatch> = Vec::new();
    let message_id = event["mail"]["messageId"]
        .as_str()
        .unwrap_or("")
        .to_string();
    for dest in destinations {
        // SNS destination
        if let Some(ref sns_dest) = dest.sns_destination {
            if let Some(topic_arn) = sns_dest["TopicArn"].as_str() {
                let message = event.to_string();
                tracing::info!(
                    topic_arn = %topic_arn,
                    event_type = ?event_type,
                    "SES event fanout -> SNS"
                );
                ctx.delivery_bus.publish_to_sns(
                    topic_arn,
                    &message,
                    Some("Amazon SES Email Event"),
                );
                dispatches.push(EventDestinationDispatch {
                    destination_name: dest.name.clone(),
                    destination_type: "sns".to_string(),
                    event_type: event_type.as_str().to_string(),
                    message_id: message_id.clone(),
                    dispatched_at: Utc::now(),
                    target_arn: topic_arn.to_string(),
                });
            }
        }

        // EventBridge destination
        if let Some(ref eb) = dest.event_bridge_destination {
            let detail = event.to_string();
            tracing::info!(
                event_type = ?event_type,
                "SES event fanout -> EventBridge"
            );
            // EventBridgeDestination.EventBusArn names the bus the events go
            // to; the events originate in the sending account and region,
            // carry the per-event detail-type and name the sending identity
            // in `resources`.
            let target_arn = eb
                .get("EventBusArn")
                .and_then(|v| v.as_str())
                .unwrap_or("default")
                .to_string();
            let resources: Vec<String> = event["mail"]["sourceArn"]
                .as_str()
                .map(|arn| vec![arn.to_string()])
                .unwrap_or_default();
            let put = ctx.delivery_bus.put_event_to_eventbridge(
                &fakecloud_core::delivery::CrossServiceEvent {
                    source: "aws.ses",
                    detail_type: event_type.eventbridge_detail_type(),
                    detail: &detail,
                    event_bus: &target_arn,
                    account_id: scope.account_id,
                    region: scope.region,
                    resources: &resources,
                    principal_arn: None,
                },
            );
            match put {
                // Refused by the target bus: not dispatched.
                Err(err) => tracing::warn!(
                    event_bus = %target_arn,
                    %err,
                    "SES event fanout -> EventBridge refused"
                ),
                Ok(_) => dispatches.push(EventDestinationDispatch {
                    destination_name: dest.name.clone(),
                    destination_type: "eventbridge".to_string(),
                    event_type: event_type.as_str().to_string(),
                    message_id: message_id.clone(),
                    dispatched_at: Utc::now(),
                    target_arn,
                }),
            }
        }

        // Kinesis / Firehose destination
        if let Some(ref kf) = dest.kinesis_firehose_destination {
            let event_json = event.to_string();
            if let Some(ds_arn) = kf
                .get("DeliveryStreamArn")
                .or_else(|| kf.get("DeliveryStreamARN"))
                .and_then(|v| v.as_str())
            {
                tracing::info!(
                    delivery_stream_arn = %ds_arn,
                    event_type = ?event_type,
                    "SES event fanout -> Firehose"
                );
                // SES follows each JSON record it publishes to Firehose with a
                // newline; Firehose itself concatenates record bytes verbatim.
                let record = format!("{event_json}\n");
                ctx.delivery_bus
                    .put_record_to_firehose(ds_arn, record.as_bytes());
                dispatches.push(EventDestinationDispatch {
                    destination_name: dest.name.clone(),
                    destination_type: "firehose".to_string(),
                    event_type: event_type.as_str().to_string(),
                    message_id: message_id.clone(),
                    dispatched_at: Utc::now(),
                    target_arn: ds_arn.to_string(),
                });
            }
            if let Some(stream_arn) = kf.get("StreamARN").and_then(|v| v.as_str()) {
                tracing::info!(
                    stream_arn = %stream_arn,
                    event_type = ?event_type,
                    "SES event fanout -> Kinesis"
                );
                let data_b64 = base64::engine::general_purpose::STANDARD.encode(&event_json);
                let partition_key = event["mail"]["messageId"].as_str().unwrap_or("default");
                ctx.delivery_bus
                    .put_record_to_kinesis(stream_arn, &data_b64, partition_key);
                dispatches.push(EventDestinationDispatch {
                    destination_name: dest.name.clone(),
                    destination_type: "kinesis".to_string(),
                    event_type: event_type.as_str().to_string(),
                    message_id: message_id.clone(),
                    dispatched_at: Utc::now(),
                    target_arn: stream_arn.to_string(),
                });
            }
        }

        // CloudWatch destination
        if let Some(ref cw) = dest.cloud_watch_destination {
            if let Some(dims) = cw.get("DimensionConfigurations").and_then(|v| v.as_array()) {
                for dim_cfg in dims {
                    let dim_name = dim_cfg["DimensionName"].as_str().unwrap_or("EventType");
                    let dim_value = dim_cfg["DefaultDimensionValue"]
                        .as_str()
                        .unwrap_or(event_type.as_str());
                    let mut dimensions = std::collections::BTreeMap::new();
                    dimensions.insert(dim_name.to_string(), dim_value.to_string());
                    tracing::info!(
                        dimension = %dim_name,
                        event_type = ?event_type,
                        "SES event fanout -> CloudWatch"
                    );
                    ctx.delivery_bus.put_cloudwatch_metric(
                        scope.account_id,
                        scope.region,
                        "AWS/SES2",
                        event_type.as_str(),
                        1.0,
                        Some("Count"),
                        dimensions,
                        Utc::now().timestamp_millis(),
                    );
                }
                dispatches.push(EventDestinationDispatch {
                    destination_name: dest.name.clone(),
                    destination_type: "cloudwatch".to_string(),
                    event_type: event_type.as_str().to_string(),
                    message_id: message_id.clone(),
                    dispatched_at: Utc::now(),
                    target_arn: String::new(),
                });
            }
        }
    }
    if !dispatches.is_empty() {
        let mut mas = ctx.ses_state.write();
        let state = mas.get_or_create(scope.account_id);
        state.event_destination_dispatches.extend(dispatches);
    }
}

/// Infer ISP name from an email address domain.
pub fn infer_isp(email: &str) -> String {
    let domain = email
        .rsplit_once('@')
        .map_or("", |(_, domain)| domain)
        .to_ascii_lowercase();
    match domain.as_str() {
        "gmail.com" => "Gmail",
        "yahoo.com" | "ymail.com" => "Yahoo",
        "hotmail.com" | "outlook.com" | "live.com" => "Outlook",
        "aol.com" => "AOL",
        "icloud.com" | "me.com" | "mac.com" => "Apple",
        "protonmail.com" => "ProtonMail",
        "zoho.com" => "Zoho",
        _ => "Other",
    }
    .to_string()
}

/// Build per-recipient delivery insights from the generated event types.
fn build_delivery_insights(
    recipient_events: &[(String, Vec<SesEventType>)],
) -> Vec<EmailRecipientInsight> {
    let mut insights = Vec::new();
    for (dest, event_types) in recipient_events {
        let mut events = Vec::new();
        for et in event_types {
            let mut ev = DeliveryInsightEvent {
                timestamp: Utc::now(),
                event_type: et.as_str().to_string(),
                ..Default::default()
            };
            match et {
                SesEventType::Bounce => {
                    ev.bounce_type = Some("Permanent".to_string());
                    ev.bounce_sub_type = Some("General".to_string());
                    ev.diagnostic_code = Some("smtp; 550 5.1.1 user unknown".to_string());
                }
                SesEventType::Complaint => {
                    ev.complaint_feedback_type = Some("abuse".to_string());
                }
                _ => {}
            }
            events.push(ev);
        }
        insights.push(EmailRecipientInsight {
            destination: dest.clone(),
            isp: infer_isp(dest),
            events,
        });
    }
    insights
}

/// Process event fanout for a sent email.
///
/// This is the main entry point called from SendEmail / SendBulkEmail.
/// It:
/// 1. Checks the suppression list for each envelope recipient
/// 2. Classifies recipients for mailbox simulator behavior
/// 3. Generates appropriate events
/// 4. Fans out to configured destinations
/// 5. Stores per-recipient delivery insights on the SentEmail
///
/// Returns `true` if any recipient was suppressed. Followup details and
/// insights apply only to the recipients with that outcome; `mail.destination`
/// always retains the complete envelope.
pub fn process_send_events(
    ctx: &SesDeliveryContext,
    scope: SendScope<'_>,
    email: &mut SentEmail,
    config_set_name: Option<&str>,
) -> bool {
    let config_set = resolve_config_set(
        &ctx.ses_state,
        scope.account_id,
        config_set_name,
        &email.from,
    );
    let recipients = envelope_recipients(email);

    let mut suppressed = false;
    let mut add_to_suppression = false;
    let recipient_events: Vec<_> = recipients
        .iter()
        .map(|recipient| {
            if config_set.as_deref().is_some_and(|cs| {
                check_suppression_list(
                    &ctx.ses_state,
                    scope.account_id,
                    std::slice::from_ref(recipient),
                    Some(cs),
                )
                .is_some()
            }) {
                suppressed = true;
                (recipient.clone(), vec![SesEventType::Bounce])
            } else {
                let (types, add) = recipient_event_types(recipient);
                add_to_suppression |= add;
                (recipient.clone(), types.to_vec())
            }
        })
        .collect();

    if suppressed {
        let mut mas = ctx.ses_state.write();
        let state = mas.get_or_create(scope.account_id);
        state.suppressed_drops_total = state.suppressed_drops_total.saturating_add(1);
    }

    // Handle suppression list addition
    if add_to_suppression {
        let mut mas = ctx.ses_state.write();
        let state = mas.get_or_create(scope.account_id);
        for addr in &recipients {
            if addr == SUPPRESSION_ADDR {
                state.suppressed_destinations.insert(
                    addr.clone(),
                    SuppressedDestination {
                        email_address: addr.clone(),
                        reason: "BOUNCE".to_string(),
                        last_update_time: Utc::now(),
                    },
                );
            }
        }
    }

    // Generate and deliver events to configured destinations
    if let Some(ref cs) = config_set {
        for event_type in [
            SesEventType::Send,
            SesEventType::Delivery,
            SesEventType::Bounce,
            SesEventType::Complaint,
        ] {
            let event_recipients: Vec<_> = recipient_events
                .iter()
                .filter(|(_, types)| types.contains(&event_type))
                .map(|(recipient, _)| recipient.clone())
                .collect();
            if !event_recipients.is_empty() {
                let event =
                    build_published_event(ctx, scope, event_type, email, cs, &event_recipients);
                deliver_event(ctx, scope, &event, event_type, cs);
            }
        }
    }

    // Store per-recipient delivery insights regardless of config set
    email.delivery_insights = build_delivery_insights(&recipient_events);

    suppressed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_normal_recipients() {
        let (events, suppress) = classify_recipients(&["user@example.com".to_string()]);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0], SesEventType::Send);
        assert_eq!(events[1], SesEventType::Delivery);
        assert!(!suppress);
    }

    #[test]
    fn classify_bounce_simulator() {
        let (events, suppress) = classify_recipients(&[BOUNCE_ADDR.to_string()]);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0], SesEventType::Send);
        assert_eq!(events[1], SesEventType::Bounce);
        assert!(!suppress);
    }

    #[test]
    fn classify_complaint_simulator() {
        let (events, suppress) = classify_recipients(&[COMPLAINT_ADDR.to_string()]);
        assert_eq!(events.len(), 3);
        assert_eq!(events[0], SesEventType::Send);
        assert_eq!(events[1], SesEventType::Delivery);
        assert_eq!(events[2], SesEventType::Complaint);
        assert!(!suppress);
    }

    #[test]
    fn classify_suppression_simulator() {
        let (events, suppress) = classify_recipients(&[SUPPRESSION_ADDR.to_string()]);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0], SesEventType::Send);
        assert_eq!(events[1], SesEventType::Bounce);
        assert!(suppress);
    }

    #[test]
    fn classify_success_simulator() {
        let (events, suppress) = classify_recipients(&[SUCCESS_ADDR.to_string()]);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0], SesEventType::Send);
        assert_eq!(events[1], SesEventType::Delivery);
        assert!(!suppress);
    }

    #[test]
    fn build_send_event_format() {
        let email = SentEmail {
            message_id: "test-msg-id".to_string(),
            from: "sender@example.com".to_string(),
            to: vec!["recipient@example.com".to_string()],
            cc: vec![],
            bcc: vec![],
            subject: Some("Hello".to_string()),
            html_body: None,
            text_body: None,
            raw_data: None,
            template_name: None,
            template_data: None,
            dkim_signature: None,
            headers: Vec::new(),
            timestamp: Utc::now(),
            email_tags: Vec::new(),
            delivery_insights: Vec::new(),
        };
        let event = build_ses_event(SesEventType::Send, &email);
        assert_eq!(event["eventType"], "Send");
        assert_eq!(event["mail"]["messageId"], "test-msg-id");
        assert_eq!(event["mail"]["source"], "sender@example.com");
        assert!(event["send"].is_object());
    }

    #[test]
    fn build_bounce_event_format() {
        let email = SentEmail {
            message_id: "bounce-msg".to_string(),
            from: "sender@example.com".to_string(),
            to: vec!["bounce@simulator.amazonses.com".to_string()],
            cc: vec![],
            bcc: vec![],
            subject: None,
            html_body: None,
            text_body: None,
            raw_data: None,
            template_name: None,
            template_data: None,
            dkim_signature: None,
            headers: Vec::new(),
            timestamp: Utc::now(),
            email_tags: Vec::new(),
            delivery_insights: Vec::new(),
        };
        let event = build_ses_event(SesEventType::Bounce, &email);
        assert_eq!(event["eventType"], "Bounce");
        assert_eq!(event["bounce"]["bounceType"], "Permanent");
        assert!(event["bounce"]["bouncedRecipients"].is_array());
    }

    #[test]
    fn build_delivery_event_format() {
        let email = SentEmail {
            message_id: "deliver-msg".to_string(),
            from: "sender@example.com".to_string(),
            to: vec!["user@example.com".to_string()],
            cc: vec![],
            bcc: vec![],
            subject: None,
            html_body: None,
            text_body: None,
            raw_data: None,
            template_name: None,
            template_data: None,
            dkim_signature: None,
            headers: Vec::new(),
            timestamp: Utc::now(),
            email_tags: Vec::new(),
            delivery_insights: Vec::new(),
        };
        let event = build_ses_event(SesEventType::Delivery, &email);
        assert_eq!(event["eventType"], "Delivery");
        assert!(event["delivery"]["timestamp"].is_string());
        assert_eq!(event["delivery"]["smtpResponse"], "250 2.0.0 Ok");
    }

    #[test]
    fn build_complaint_event_format() {
        let email = SentEmail {
            message_id: "complaint-msg".to_string(),
            from: "sender@example.com".to_string(),
            to: vec!["complaint@simulator.amazonses.com".to_string()],
            cc: vec![],
            bcc: vec![],
            subject: None,
            html_body: None,
            text_body: None,
            raw_data: None,
            template_name: None,
            template_data: None,
            dkim_signature: None,
            headers: Vec::new(),
            timestamp: Utc::now(),
            email_tags: Vec::new(),
            delivery_insights: Vec::new(),
        };
        let event = build_ses_event(SesEventType::Complaint, &email);
        assert_eq!(event["eventType"], "Complaint");
        assert_eq!(event["complaint"]["complaintFeedbackType"], "abuse");
    }

    #[test]
    fn classify_multiple_recipients_no_simulator() {
        let recipients = vec![
            "a@example.com".to_string(),
            "b@example.com".to_string(),
            "c@example.com".to_string(),
        ];
        let (events, suppress) = classify_recipients(&recipients);
        assert!(events.contains(&SesEventType::Send));
        assert!(events.contains(&SesEventType::Delivery));
        assert!(!suppress);
    }

    #[test]
    fn classify_empty_recipients() {
        let (events, suppress) = classify_recipients(&[]);
        assert!(!events.is_empty());
        assert!(!suppress);
    }

    fn shared_state() -> SharedSesState {
        use fakecloud_core::multi_account::MultiAccountState;
        Arc::new(parking_lot::RwLock::new(MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4566",
        )))
    }

    #[test]
    fn check_suppression_list_finds_suppressed() {
        let state = shared_state();
        state.write().default_mut().suppressed_destinations.insert(
            "blocked@example.com".to_string(),
            SuppressedDestination {
                email_address: "blocked@example.com".to_string(),
                reason: "BOUNCE".to_string(),
                last_update_time: Utc::now(),
            },
        );
        let hit = check_suppression_list(
            &state,
            "123456789012",
            &[
                "ok@example.com".to_string(),
                "blocked@example.com".to_string(),
            ],
            None,
        );
        assert_eq!(hit.as_deref(), Some("blocked@example.com"));
    }

    #[test]
    fn check_suppression_list_none_when_clean() {
        let state = shared_state();
        let hit = check_suppression_list(
            &state,
            "123456789012",
            &["ok@example.com".to_string()],
            None,
        );
        assert!(hit.is_none());
    }

    #[test]
    fn check_suppression_list_skips_when_reason_filter_excludes() {
        // Address suppressed for COMPLAINT, but the resolved config set
        // only enforces BOUNCE — fanout must not bounce that recipient.
        let state = shared_state();
        {
            let mut mas = state.write();
            let st = mas.default_mut();
            st.suppressed_destinations.insert(
                "blocked@example.com".to_string(),
                SuppressedDestination {
                    email_address: "blocked@example.com".to_string(),
                    reason: "COMPLAINT".to_string(),
                    last_update_time: Utc::now(),
                },
            );
            st.configuration_sets.insert(
                "bounce-only".to_string(),
                crate::state::ConfigurationSet {
                    name: "bounce-only".to_string(),
                    sending_enabled: true,
                    tls_policy: "OPTIONAL".to_string(),
                    sending_pool_name: None,
                    max_delivery_seconds: None,
                    custom_redirect_domain: None,
                    https_policy: None,
                    suppressed_reasons: vec!["BOUNCE".to_string()],
                    reputation_metrics_enabled: false,
                    vdm_options: None,
                    archive_arn: None,
                    message_security_options: None,
                    archiving_options_present: false,
                },
            );
        }
        let hit = check_suppression_list(
            &state,
            "123456789012",
            &["blocked@example.com".to_string()],
            Some("bounce-only"),
        );
        assert!(hit.is_none());
    }

    #[test]
    fn check_suppression_list_account_fallback_when_config_set_empty() {
        // Config set has no suppressed_reasons; fall back to account
        // scope which only enforces COMPLAINT.
        let state = shared_state();
        {
            let mut mas = state.write();
            let st = mas.default_mut();
            st.suppressed_destinations.insert(
                "blocked@example.com".to_string(),
                SuppressedDestination {
                    email_address: "blocked@example.com".to_string(),
                    reason: "BOUNCE".to_string(),
                    last_update_time: Utc::now(),
                },
            );
            st.account_settings.suppressed_reasons = vec!["COMPLAINT".to_string()];
            st.configuration_sets.insert(
                "passthrough".to_string(),
                crate::state::ConfigurationSet {
                    name: "passthrough".to_string(),
                    sending_enabled: true,
                    tls_policy: "OPTIONAL".to_string(),
                    sending_pool_name: None,
                    max_delivery_seconds: None,
                    custom_redirect_domain: None,
                    https_policy: None,
                    suppressed_reasons: Vec::new(),
                    reputation_metrics_enabled: false,
                    vdm_options: None,
                    archive_arn: None,
                    message_security_options: None,
                    archiving_options_present: false,
                },
            );
        }
        let hit = check_suppression_list(
            &state,
            "123456789012",
            &["blocked@example.com".to_string()],
            Some("passthrough"),
        );
        assert!(hit.is_none());
    }

    #[test]
    fn check_suppression_list_case_insensitive() {
        let state = shared_state();
        state.write().default_mut().suppressed_destinations.insert(
            "Blocked@Example.com".to_string(),
            SuppressedDestination {
                email_address: "Blocked@Example.com".to_string(),
                reason: "BOUNCE".to_string(),
                last_update_time: Utc::now(),
            },
        );
        let hit = check_suppression_list(
            &state,
            "123456789012",
            &["BLOCKED@example.COM".to_string()],
            None,
        );
        assert_eq!(hit.as_deref(), Some("BLOCKED@example.COM"));
    }

    fn make_identity(name: &str, config_set: Option<&str>) -> crate::state::EmailIdentity {
        crate::state::EmailIdentity {
            identity_name: name.to_string(),
            identity_type: "EmailAddress".to_string(),
            verified: true,
            created_at: Utc::now(),
            dkim_signing_enabled: false,
            dkim_signing_attributes_origin: "AWS_SES".to_string(),
            dkim_domain_signing_private_key: None,
            dkim_domain_signing_selector: None,
            dkim_next_signing_key_length: None,
            dkim_public_key_b64: None,
            email_forwarding_enabled: true,
            mail_from_domain: None,
            mail_from_behavior_on_mx_failure: "USE_DEFAULT_VALUE".to_string(),
            mail_from_domain_status: "NotStarted".to_string(),
            configuration_set_name: config_set.map(|s| s.to_string()),
            bounce_topic: None,
            complaint_topic: None,
            delivery_topic: None,
            verification_token: None,
        }
    }

    #[test]
    fn resolve_config_set_uses_explicit_arg_first() {
        let state = shared_state();
        let resolved =
            resolve_config_set(&state, "123456789012", Some("my-cs"), "sender@example.com");
        assert_eq!(resolved.as_deref(), Some("my-cs"));
    }

    #[test]
    fn resolve_config_set_uses_identity_default_when_no_explicit() {
        let state = shared_state();
        state.write().default_mut().identities.insert(
            "sender@example.com".to_string(),
            make_identity("sender@example.com", Some("identity-cs")),
        );
        let resolved = resolve_config_set(&state, "123456789012", None, "sender@example.com");
        assert_eq!(resolved.as_deref(), Some("identity-cs"));
    }

    #[test]
    fn resolve_config_set_falls_back_to_domain_identity() {
        let state = shared_state();
        state.write().default_mut().identities.insert(
            "example.com".to_string(),
            make_identity("example.com", Some("domain-cs")),
        );
        let resolved = resolve_config_set(&state, "123456789012", None, "sender@example.com");
        assert_eq!(resolved.as_deref(), Some("domain-cs"));
    }

    #[test]
    fn resolve_config_set_none_when_nothing_set() {
        let state = shared_state();
        assert!(resolve_config_set(&state, "123456789012", None, "sender@example.com").is_none());
    }

    #[test]
    fn get_matching_destinations_filters_by_enabled_and_event_type() {
        let state = shared_state();
        state.write().default_mut().event_destinations.insert(
            "cs".to_string(),
            vec![
                EventDestination {
                    name: "sns-dest".to_string(),
                    enabled: true,
                    matching_event_types: vec!["SEND".to_string(), "BOUNCE".to_string()],
                    kinesis_firehose_destination: None,
                    cloud_watch_destination: None,
                    sns_destination: Some(serde_json::json!({"TopicArn": "arn"})),
                    event_bridge_destination: None,
                    pinpoint_destination: None,
                },
                EventDestination {
                    name: "disabled".to_string(),
                    enabled: false,
                    matching_event_types: vec!["SEND".to_string()],
                    kinesis_firehose_destination: None,
                    cloud_watch_destination: None,
                    sns_destination: None,
                    event_bridge_destination: None,
                    pinpoint_destination: None,
                },
            ],
        );
        let dests = get_matching_destinations(&state, "123456789012", "cs", "SEND");
        assert_eq!(dests.len(), 1);
        assert_eq!(dests[0].name, "sns-dest");
        let none = get_matching_destinations(&state, "123456789012", "cs", "DELIVERY");
        assert!(none.is_empty());
        let missing = get_matching_destinations(&state, "123456789012", "unknown", "SEND");
        assert!(missing.is_empty());
    }

    type EbCall = (
        String,
        String,
        String,
        String,
        Vec<String>,
        serde_json::Value,
    );

    #[derive(Default)]
    struct EbRecorder(std::sync::Mutex<Vec<EbCall>>);

    impl fakecloud_core::delivery::EventBridgeDelivery for EbRecorder {
        fn put_event(
            &self,
            e: &fakecloud_core::delivery::CrossServiceEvent<'_>,
        ) -> Result<String, fakecloud_core::delivery::EventBridgeDeliveryError> {
            self.0.lock().unwrap().push((
                e.detail_type.to_string(),
                e.event_bus.to_string(),
                e.account_id.to_string(),
                e.region.to_string(),
                e.resources.to_vec(),
                serde_json::from_str(e.detail).unwrap(),
            ));
            Ok("id".to_string())
        }
    }

    /// A send in a non-default account/region is fanned out with that
    /// account's configuration set; each EventBridge event carries the
    /// per-event SES detail-type, the sending identity in `resources`, and
    /// the published `mail` fields.
    #[test]
    fn eventbridge_fanout_uses_send_scope_and_aws_detail_types() {
        let state = shared_state();
        state
            .write()
            .get_or_create("111111111111")
            .event_destinations
            .insert(
                "cs".to_string(),
                vec![EventDestination {
                    name: "eb".to_string(),
                    enabled: true,
                    matching_event_types: vec!["SEND".to_string(), "DELIVERY".to_string()],
                    kinesis_firehose_destination: None,
                    cloud_watch_destination: None,
                    sns_destination: None,
                    event_bridge_destination: Some(serde_json::json!({
                        "EventBusArn": "arn:aws:events:eu-west-1:111111111111:event-bus/default"
                    })),
                    pinpoint_destination: None,
                }],
            );
        let recorder = Arc::new(EbRecorder::default());
        let ctx = SesDeliveryContext {
            ses_state: state,
            delivery_bus: Arc::new(DeliveryBus::new().with_eventbridge(recorder.clone())),
        };
        let mut email = SentEmail {
            message_id: "msg-1".to_string(),
            from: "Sender <sender@example.com>".to_string(),
            to: vec!["recipient@example.com".to_string()],
            cc: vec![],
            bcc: vec![],
            subject: None,
            html_body: None,
            text_body: None,
            raw_data: None,
            template_name: None,
            template_data: None,
            dkim_signature: None,
            headers: Vec::new(),
            timestamp: Utc::now(),
            email_tags: vec![("campaign".to_string(), "fall".to_string())],
            delivery_insights: Vec::new(),
        };
        process_send_events(
            &ctx,
            SendScope {
                account_id: "111111111111",
                region: "eu-west-1",
            },
            &mut email,
            Some("cs"),
        );
        let calls = recorder.0.lock().unwrap();
        let types: Vec<&str> = calls.iter().map(|c| c.0.as_str()).collect();
        assert_eq!(types, vec!["Email Sent", "Email Delivered"]);
        let (_, bus, account, region, resources, detail) = &calls[0];
        assert_eq!(
            bus,
            "arn:aws:events:eu-west-1:111111111111:event-bus/default"
        );
        assert_eq!(account, "111111111111");
        assert_eq!(region, "eu-west-1");
        let identity_arn = "arn:aws:ses:eu-west-1:111111111111:identity/example.com";
        assert_eq!(resources, &vec![identity_arn.to_string()]);
        assert_eq!(detail["eventType"], "Send");
        assert_eq!(detail["mail"]["sourceArn"], identity_arn);
        assert_eq!(detail["mail"]["sendingAccountId"], "111111111111");
        assert_eq!(detail["mail"]["headersTruncated"], false);
        assert_eq!(
            detail["mail"]["tags"]["ses:configuration-set"],
            serde_json::json!(["cs"])
        );
        assert_eq!(
            detail["mail"]["tags"]["campaign"],
            serde_json::json!(["fall"])
        );
    }

    #[test]
    fn deliver_event_kinesis_firehose_cloudwatch_no_panic() {
        let state = shared_state();
        state.write().default_mut().event_destinations.insert(
            "cs".to_string(),
            vec![EventDestination {
                name: "multi-dest".to_string(),
                enabled: true,
                matching_event_types: vec!["SEND".to_string()],
                kinesis_firehose_destination: Some(serde_json::json!({
                    "DeliveryStreamARN": "arn:aws:firehose:us-east-1:123456789012:deliverystream/my-stream",
                    "StreamARN": "arn:aws:kinesis:us-east-1:123456789012:stream/my-kinesis"
                })),
                cloud_watch_destination: Some(serde_json::json!({
                    "DimensionConfigurations": [
                        {
                            "DimensionName": "EventType",
                            "DimensionValueSource": "MESSAGE_TAG",
                            "DefaultDimensionValue": "Send"
                        }
                    ]
                })),
                sns_destination: None,
                event_bridge_destination: None,
                pinpoint_destination: None,
            }],
        );
        let ctx = SesDeliveryContext {
            ses_state: state,
            delivery_bus: Arc::new(DeliveryBus::new()),
        };
        let event = build_ses_event(
            SesEventType::Send,
            &SentEmail {
                message_id: "msg-1".to_string(),
                from: "sender@example.com".to_string(),
                to: vec!["recipient@example.com".to_string()],
                cc: vec![],
                bcc: vec![],
                subject: None,
                html_body: None,
                text_body: None,
                raw_data: None,
                template_name: None,
                template_data: None,
                dkim_signature: None,
                headers: Vec::new(),
                timestamp: Utc::now(),
                email_tags: Vec::new(),
                delivery_insights: Vec::new(),
            },
        );
        // No senders wired — must not panic.
        deliver_event(
            &ctx,
            SendScope {
                account_id: "123456789012",
                region: "us-east-1",
            },
            &event,
            SesEventType::Send,
            "cs",
        );
    }

    #[derive(Default)]
    struct FirehoseRecorder(std::sync::Mutex<Vec<(String, Vec<u8>)>>);

    impl fakecloud_core::delivery::FirehoseDelivery for FirehoseRecorder {
        fn put_record(&self, delivery_stream_arn: &str, data: &[u8]) {
            self.0
                .lock()
                .unwrap()
                .push((delivery_stream_arn.to_string(), data.to_vec()));
        }
    }

    /// AWS: "When publishing events to Firehose, Amazon SES follows each JSON
    /// record with a newline character." Firehose itself concatenates record
    /// bytes verbatim, so the delimiter must come from the SES record.
    #[test]
    fn firehose_event_records_end_with_a_newline() {
        let state = shared_state();
        let ds_arn = "arn:aws:firehose:us-east-1:123456789012:deliverystream/ses-events";
        state.write().default_mut().event_destinations.insert(
            "cs".to_string(),
            vec![EventDestination {
                name: "fh".to_string(),
                enabled: true,
                matching_event_types: vec!["SEND".to_string()],
                kinesis_firehose_destination: Some(serde_json::json!({
                    "DeliveryStreamARN": ds_arn,
                    "IamRoleARN": "arn:aws:iam::123456789012:role/ses-firehose"
                })),
                cloud_watch_destination: None,
                sns_destination: None,
                event_bridge_destination: None,
                pinpoint_destination: None,
            }],
        );
        let recorder = Arc::new(FirehoseRecorder::default());
        let ctx = SesDeliveryContext {
            ses_state: state,
            delivery_bus: Arc::new(DeliveryBus::new().with_firehose(recorder.clone())),
        };
        let event = build_ses_event(
            SesEventType::Send,
            &SentEmail {
                message_id: "msg-1".to_string(),
                from: "sender@example.com".to_string(),
                to: vec!["recipient@example.com".to_string()],
                cc: vec![],
                bcc: vec![],
                subject: None,
                html_body: None,
                text_body: None,
                raw_data: None,
                template_name: None,
                template_data: None,
                dkim_signature: None,
                headers: Vec::new(),
                timestamp: Utc::now(),
                email_tags: Vec::new(),
                delivery_insights: Vec::new(),
            },
        );
        deliver_event(
            &ctx,
            SendScope {
                account_id: "123456789012",
                region: "us-east-1",
            },
            &event,
            SesEventType::Send,
            "cs",
        );

        let records = recorder.0.lock().unwrap();
        assert_eq!(records.len(), 1);
        let (arn, data) = &records[0];
        assert_eq!(arn, ds_arn);
        let json = data
            .strip_suffix(b"\n")
            .expect("SES Firehose record must end with a newline");
        assert!(!json.ends_with(b"\n"), "exactly one trailing newline");
        let parsed: serde_json::Value = serde_json::from_slice(json).unwrap();
        assert_eq!(parsed, event);
    }

    #[test]
    fn firehose_fanout_accepts_both_api_destination_spellings_and_frames_records() {
        for key in ["DeliveryStreamArn", "DeliveryStreamARN"] {
            let state = shared_state();
            let arn = "arn:aws:firehose:us-east-1:123456789012:deliverystream/events";
            state.write().default_mut().event_destinations.insert(
                "cs".to_string(),
                vec![EventDestination {
                    name: "fh".to_string(),
                    enabled: true,
                    matching_event_types: vec!["SEND".to_string()],
                    kinesis_firehose_destination: Some(json!({key: arn})),
                    cloud_watch_destination: None,
                    sns_destination: None,
                    event_bridge_destination: None,
                    pinpoint_destination: None,
                }],
            );
            let recorder = Arc::new(FirehoseRecorder::default());
            let ctx = SesDeliveryContext {
                ses_state: state,
                delivery_bus: Arc::new(DeliveryBus::new().with_firehose(recorder.clone())),
            };
            let event = json!({"eventType": "Send", "mail": {"messageId": "id"}, "send": {}});
            deliver_event(
                &ctx,
                SendScope {
                    account_id: "123456789012",
                    region: "us-east-1",
                },
                &event,
                SesEventType::Send,
                "cs",
            );
            let calls = recorder.0.lock().unwrap();
            assert_eq!(calls.len(), 1, "destination key {key}");
            assert_eq!(calls[0].0, arn);
            assert_eq!(calls[0].1, format!("{event}\n").as_bytes());
        }
    }

    #[test]
    fn matching_destinations_accept_v1_event_types_without_mutating_them() {
        let state = shared_state();
        let types = vec![
            "send",
            "reject",
            "bounce",
            "complaint",
            "delivery",
            "open",
            "click",
            "renderingFailure",
            "DELIVERY_DELAY",
            "SUBSCRIPTION",
        ];
        state.write().default_mut().event_destinations.insert(
            "cs".to_string(),
            vec![EventDestination {
                name: "v1".to_string(),
                enabled: true,
                matching_event_types: types.iter().map(|s| s.to_string()).collect(),
                kinesis_firehose_destination: None,
                cloud_watch_destination: None,
                sns_destination: Some(json!({"TopicArn": "arn"})),
                event_bridge_destination: None,
                pinpoint_destination: None,
            }],
        );
        for event in [
            "SEND",
            "REJECT",
            "BOUNCE",
            "COMPLAINT",
            "DELIVERY",
            "OPEN",
            "CLICK",
            "RENDERING_FAILURE",
            "DELIVERY_DELAY",
            "SUBSCRIPTION",
        ] {
            let matched = get_matching_destinations(&state, "123456789012", "cs", event);
            assert_eq!(matched.len(), 1, "{event:?}");
            assert_eq!(matched[0].matching_event_types, types);
        }
        assert!(get_matching_destinations(&state, "123456789012", "cs", "UNKNOWN").is_empty());
    }
}
