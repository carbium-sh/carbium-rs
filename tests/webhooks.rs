//! The webhook check against the Standard Webhooks specification's own published example, an answer this crate did
//! not compute; then a one-byte body change, a re-encoded body, a wrong secret, the clock window, header names in any
//! case, and a clock window that fails closed.
#![cfg(feature = "webhooks")]

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use carbium::webhooks::VerifyOptions;
use carbium::{ErrorKind, verify_webhook, verify_webhook_signature};
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

/// The specification's example secret, joined here so no file holds it whole.
fn secret() -> String {
    ["whsec", "_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"].concat()
}
const ID: &str = "msg_p5jXN8AQM9LWM0D4loKWxJek";
const STAMP: &str = "1614265330";
const BODY: &[u8] = br#"{"test": 2432232314}"#;
const SIGNATURE: &str = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=";

fn at(seconds: i64) -> VerifyOptions {
    let now = if seconds >= 0 {
        UNIX_EPOCH + Duration::from_secs(seconds as u64)
    } else {
        UNIX_EPOCH - Duration::from_secs(seconds.unsigned_abs())
    };
    VerifyOptions {
        now: Some(now),
        ..Default::default()
    }
}

fn stamp_clock() -> VerifyOptions {
    at(STAMP.parse().unwrap())
}

fn headers(signature: &str, stamp: &str, id: &str) -> Vec<(String, String)> {
    vec![
        ("webhook-id".into(), id.into()),
        ("webhook-timestamp".into(), stamp.into()),
        ("webhook-signature".into(), signature.into()),
    ]
}

fn published() -> Vec<(String, String)> {
    headers(SIGNATURE, STAMP, ID)
}

/// A v1 entry for a body this test made up, so a path past the signature can be reached. The published example
/// above is the oracle; this only shapes fixtures.
fn sign(id: &str, stamp: &str, body: &[u8]) -> String {
    let key = BASE64.decode(&secret()[6..]).unwrap();
    let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(&key).unwrap();
    mac.update(format!("{id}.{stamp}.").as_bytes());
    mac.update(body);
    format!("v1,{}", BASE64.encode(mac.finalize().into_bytes()))
}

fn reason(result: carbium::Result<&[u8]>) -> String {
    match result {
        Ok(_) => "verified".into(),
        Err(e) => {
            assert_eq!(e.kind(), ErrorKind::Webhook);
            e.reason().unwrap_or("none").into()
        }
    }
}

fn check(body: &[u8], h: &[(String, String)], secret: &str) -> String {
    reason(verify_webhook_signature(body, h, secret, &stamp_clock()))
}

#[test]
fn the_published_example_verifies_from_a_list_a_header_map_and_a_hash_map() {
    assert_eq!(
        verify_webhook_signature(BODY, published().as_slice(), &secret(), &stamp_clock()).unwrap(),
        BODY
    );
    let mut map = reqwest::header::HeaderMap::new();
    for (k, v) in published() {
        map.insert(
            reqwest::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
            v.parse().unwrap(),
        );
    }
    assert_eq!(
        reason(verify_webhook_signature(
            BODY,
            &map,
            &secret(),
            &stamp_clock()
        )),
        "verified"
    );
    let hash: HashMap<String, String> = published().into_iter().collect();
    assert_eq!(
        reason(verify_webhook_signature(
            BODY,
            &hash,
            &secret(),
            &stamp_clock()
        )),
        "verified"
    );
    let events =
        verify_webhook(BODY, published().as_slice(), &secret(), &stamp_clock()).unwrap_err();
    assert_eq!(
        events.reason(),
        Some("bad_json"),
        "a Carbium body is a list"
    );
}

#[test]
fn header_names_match_in_any_case() {
    let cased = [
        ("Webhook-Id", ID),
        ("Webhook-Timestamp", STAMP),
        ("Webhook-Signature", SIGNATURE),
    ];
    assert_eq!(
        reason(verify_webhook_signature(
            BODY,
            &cased,
            &secret(),
            &stamp_clock()
        )),
        "verified"
    );
    let upper: HashMap<&str, &str> = [
        ("WEBHOOK-ID", ID),
        ("WEBHOOK-TIMESTAMP", STAMP),
        ("WEBHOOK-SIGNATURE", SIGNATURE),
    ]
    .into_iter()
    .collect();
    assert_eq!(
        reason(verify_webhook_signature(
            BODY,
            &upper,
            &secret(),
            &stamp_clock()
        )),
        "verified"
    );
}

#[test]
fn one_changed_byte_anywhere_or_a_re_encoded_body_fails() {
    for position in [0, BODY.len() / 2, BODY.len() - 1] {
        let mut flipped = BODY.to_vec();
        flipped[position] ^= 1;
        assert_eq!(
            check(&flipped, &published(), &secret()),
            "no_match",
            "byte {position}"
        );
    }
    assert_eq!(
        check(&[BODY, b" "].concat(), &published(), &secret()),
        "no_match"
    );
    assert_eq!(
        check(br#"{"test":2432232314}"#, &published(), &secret()),
        "no_match",
        "the same JSON, encoded again"
    );
}

#[test]
fn a_changed_id_or_timestamp_a_wrong_secret_and_bad_headers_each_fail_with_their_reason() {
    assert_eq!(
        check(BODY, &headers(SIGNATURE, STAMP, "msg_other"), &secret()),
        "no_match"
    );
    let next = (STAMP.parse::<u64>().unwrap() + 1).to_string();
    assert_eq!(
        check(BODY, &headers(SIGNATURE, &next, ID), &secret()),
        "no_match"
    );
    let wrong = format!("whsec_{}", BASE64.encode(b"another secret"));
    assert_eq!(check(BODY, &published(), &wrong), "no_match");
    assert_eq!(
        check(BODY, &headers("v1,AAAA", STAMP, ID), &secret()),
        "no_match"
    );
    assert_eq!(check(BODY, &published(), &secret()[6..]), "bad_secret");
    assert_eq!(check(BODY, &published(), "whsec_"), "bad_secret");
    assert_eq!(check(BODY, &published(), "whsec_not base64!"), "bad_secret");
    assert_eq!(check(BODY, &published()[..2], &secret()), "missing_header");
    assert_eq!(
        check(BODY, &headers(SIGNATURE, "soon", ID), &secret()),
        "bad_timestamp"
    );
    assert_eq!(
        check(BODY, &headers(SIGNATURE, "1e9", ID), &secret()),
        "bad_timestamp"
    );
}

#[test]
fn the_clock_window_is_300_seconds_either_way_and_a_custom_tolerance_is_honoured() {
    let stamp: i64 = STAMP.parse().unwrap();
    for (skew, expected) in [
        (300, "verified"),
        (-300, "verified"),
        (301, "stale"),
        (-301, "stale"),
    ] {
        assert_eq!(
            reason(verify_webhook_signature(
                BODY,
                published().as_slice(),
                &secret(),
                &at(stamp + skew)
            )),
            expected,
            "skew {skew}"
        );
    }
    let wide = VerifyOptions {
        tolerance: Duration::from_secs(600),
        ..at(stamp + 400)
    };
    assert_eq!(
        reason(verify_webhook_signature(
            BODY,
            published().as_slice(),
            &secret(),
            &wide
        )),
        "verified"
    );
}

#[test]
fn a_timestamp_or_clock_that_cannot_be_compared_is_refused_as_stale() {
    let huge = "9".repeat(30); // past u64: it cannot be read as a time
    let signed = headers(&sign(ID, &huge, BODY), &huge, ID);
    assert_eq!(check(BODY, &signed, &secret()), "stale");
    let before_1970 = VerifyOptions {
        now: Some(UNIX_EPOCH - Duration::from_secs(1)),
        ..Default::default()
    };
    assert_eq!(
        reason(verify_webhook_signature(
            BODY,
            published().as_slice(),
            &secret(),
            &before_1970
        )),
        "stale"
    );
    let zero = headers(&sign(ID, "0", BODY), "0", ID);
    assert_eq!(
        check(BODY, &zero, &secret()),
        "stale",
        "1970 is far from the clock"
    );
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string();
    let fresh = headers(&sign(ID, &now, BODY), &now, ID);
    assert_eq!(
        reason(verify_webhook_signature(
            BODY,
            fresh.as_slice(),
            &secret(),
            &VerifyOptions::default()
        )),
        "verified",
        "the system clock is read when none is given"
    );
}

#[test]
fn either_entry_matches_while_a_secret_rotates_and_a_list_body_returns_its_events() {
    assert_eq!(
        check(
            BODY,
            &headers(&format!("v1,AAAA {SIGNATURE}"), STAMP, ID),
            &secret()
        ),
        "verified"
    );
    assert_eq!(
        check(
            BODY,
            &headers(&format!("{SIGNATURE} v1,AAAA"), STAMP, ID),
            &secret()
        ),
        "verified"
    );
    assert_eq!(
        check(
            BODY,
            &headers(&format!("v2,{}", &SIGNATURE[3..]), STAMP, ID),
            &secret()
        ),
        "no_match"
    );
    assert_eq!(
        check(BODY, &headers("v1,AAAA v1,BBBB", STAMP, ID), &secret()),
        "no_match"
    );
    let body = br#"[{"slot": 1, "signature": "abc"}, {"slot": 0, "test": true}]"#;
    let events = verify_webhook(
        body,
        headers(&sign(ID, STAMP, body), STAMP, ID).as_slice(),
        &secret(),
        &stamp_clock(),
    )
    .unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(
        (events[0].slot.as_u64(), events[0].signature.as_deref()),
        (Some(1), Some("abc"))
    );
    assert_eq!(events[1].test, Some(true));
}

#[test]
fn the_secret_never_shows_in_an_error() {
    let e = verify_webhook_signature(
        BODY,
        &headers("v1,AAAA", STAMP, ID),
        &secret(),
        &stamp_clock(),
    )
    .unwrap_err();
    let shown = format!("{e} {e:?}");
    assert!(!shown.contains(&secret()[6..]), "{shown}");
}
