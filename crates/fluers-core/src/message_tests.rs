//! Tests for the base64 serde module (via the public `ImageContent` surface).

#![cfg(test)]

use super::ImageContent;
use proptest::prelude::*;
use serde_json::json;

proptest! {
    /// Serialize → deserialize round-trips for arbitrary image bytes.
    #[test]
    fn image_roundtrip(bytes in prop::collection::vec(any::<u8>(), 0..4096)) {
        let img = ImageContent { media_type: "image/png".into(), data: bytes.clone() };
        let v = serde_json::to_value(&img).unwrap();
        let back: ImageContent = serde_json::from_value(v).unwrap();
        prop_assert_eq!(back.data, bytes);
    }
}

#[test]
fn image_serializes_known_bytes() {
    // 0xFF is the classic edge case for hand-rolled base64 ("/w==").
    let img = ImageContent {
        media_type: "image/png".into(),
        data: vec![0xFF],
    };
    let v = serde_json::to_value(&img).unwrap();
    assert_eq!(v["data"], "/w==");
    let back: ImageContent = serde_json::from_value(v).unwrap();
    assert_eq!(back.data, vec![0xFF]);
}

#[test]
fn image_empty_data_roundtrips() {
    let img = ImageContent {
        media_type: "image/png".into(),
        data: Vec::new(),
    };
    let v = serde_json::to_value(&img).unwrap();
    assert_eq!(v["data"], "");
    let back: ImageContent = serde_json::from_value(v).unwrap();
    assert!(back.data.is_empty());
}

#[test]
fn image_invalid_base64_errors() {
    let bad = json!({ "media_type": "image/png", "data": "!!!not-base64!!!" });
    let res: Result<ImageContent, _> = serde_json::from_value(bad);
    assert!(res.is_err());
}
