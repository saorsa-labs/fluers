//! Skills — `SKILL.md` loading and packaged-skill directories.
//!
//! Mirrors Flue's skill loading (`Skill`, `PackagedSkillDirectory`, the
//! `/.flue/packaged-skills/` convention).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::{RuntimeError, RuntimeResult};

/// Where packaged skills live inside a project.
pub const PACKAGED_SKILLS_ROOT: &str = "/.flue/packaged-skills/";

/// A loaded skill.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Skill {
    /// Skill name (from frontmatter `name`).
    pub name: String,
    /// One-line description (from frontmatter `description`).
    pub description: String,
    /// The full markdown body.
    pub body: String,
    /// Where it was loaded from.
    pub source: PathBuf,
}

impl Skill {
    /// Load a skill from a `SKILL.md` file.
    ///
    /// MVP: parses only `name:` and `description:` frontmatter keys; the full
    /// frontmatter schema (triggers, model, etc.) lands later.
    pub async fn load(path: impl AsRef<Path>) -> RuntimeResult<Arc<Self>> {
        let path = path.as_ref();
        let raw = tokio::fs::read_to_string(path)
            .await
            .map_err(RuntimeError::Io)?;
        let (front, body) = split_frontmatter(&raw);
        let name = front
            .iter()
            .find(|(k, _)| k == "name")
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| {
                path.file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("skill")
                    .to_string()
            });
        let description = front
            .iter()
            .find(|(k, _)| k == "description")
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
        Ok(Arc::new(Self {
            name,
            description,
            body: body.to_string(),
            source: path.to_path_buf(),
        }))
    }
}

/// Split `---\nkey: val\n---\nbody` into `(frontmatter pairs, body)`.
pub(crate) fn split_frontmatter(raw: &str) -> (Vec<(String, String)>, &str) {
    let raw = raw.strip_prefix("---\n").unwrap_or(raw);
    let Some(end) = raw.find("\n---\n") else {
        return (Vec::new(), raw);
    };
    let front = &raw[..end];
    let body = &raw[end + "\n---\n".len()..];
    let pairs = front
        .lines()
        .filter_map(|line| line.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect();
    (pairs, body)
}
