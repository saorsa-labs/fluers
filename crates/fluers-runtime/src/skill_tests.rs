//! Tests for skill loading.

#![cfg(test)]

use super::skill::{split_frontmatter, Skill};
use std::io::Write;

#[test]
fn split_frontmatter_basic() {
    let raw = "---\nname: triage\ndescription: Triage bugs\n---\n# Body\nDo the thing.";
    let (front, body) = split_frontmatter(raw);
    let names: Vec<_> = front.iter().map(|(k, _)| k.as_str()).collect();
    assert!(names.contains(&"name"));
    assert!(names.contains(&"description"));
    assert_eq!(body, "# Body\nDo the thing.");
}

#[test]
fn split_frontmatter_value_with_colon_preserved() {
    // split_once splits on the FIRST ':' only — values may contain colons.
    let raw = "---\nname: triage\ndescription: http://example.com:8080\n---\nbody";
    let (front, _body) = split_frontmatter(raw);
    let desc = front
        .iter()
        .find(|(k, _)| k == "description")
        .map(|(_, v)| v.as_str())
        .unwrap();
    assert_eq!(desc, "http://example.com:8080");
}

#[test]
fn split_frontmatter_no_frontmatter() {
    let raw = "just a body, no frontmatter";
    let (front, body) = split_frontmatter(raw);
    assert!(front.is_empty());
    assert_eq!(body, raw);
}

#[tokio::test]
async fn skill_load_parses_name_and_description() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("SKILL.md");
    let mut f = std::fs::File::create(&path).unwrap();
    writeln!(
        f,
        "---\nname: verify\ndescription: Verify a fix\n---\n# Verify\nRun the tests."
    )
    .unwrap();
    drop(f);

    let skill = Skill::load(&path).await.unwrap();
    assert_eq!(skill.name, "verify");
    assert_eq!(skill.description, "Verify a fix");
    assert!(skill.body.contains("# Verify"));
}
