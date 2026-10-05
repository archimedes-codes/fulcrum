//! The composed model key (`<provider>/<id>`) and its optional
//! `:<level>` thinking-level suffix, as named types (Task 2 of the
//! god-file decomposition — the "model ids may contain `/`" rule and the
//! "the level is the trailing segment" rule each had a comment home at
//! every parse site; now they have ONE home).

use std::fmt;

/// A composed model key `<provider>/<id>`. Provider ids do not contain
/// `/`; model ids MAY, so the key splits on the FIRST `/` (the documented
/// rule, `resolve_composed_model`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModelKey {
    pub provider: String,
    pub id: String,
}

impl ModelKey {
    pub fn new(provider: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            id: id.into(),
        }
    }

    /// Parse a composed key. Splits on the FIRST `/` (provider ids do not
    /// contain `/`; model ids MAY). `None` when there is no `/`. An empty
    /// provider is structurally a valid split (provider `""`) — whether the
    /// key names a real model is the CALLER's job (the catalog lookup), not
    /// the parser's (mirrors the old `split_once('/')` + catalog-find).
    pub fn parse(s: &str) -> Option<Self> {
        let (provider, id) = s.split_once('/')?;
        Some(ModelKey {
            provider: provider.into(),
            id: id.into(),
        })
    }
}

impl fmt::Display for ModelKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.provider, self.id)
    }
}

/// A model key + optional thinking level (`<provider>/<id>:<level>`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelRef {
    pub key: ModelKey,
    pub level: Option<String>,
}

impl ModelRef {
    /// Parse a ref. Splits on the LAST `:` (a model id may contain `:`, so
    /// the level is always the trailing segment — the bare part must still
    /// be a valid key, so a ref without `/` parses to `None`).
    pub fn parse(s: &str) -> Option<Self> {
        let (bare, level) = match s.rsplit_once(':') {
            Some((b, l)) => (b, Some(l.to_string())),
            None => (s, None),
        };
        let key = ModelKey::parse(bare)?;
        Some(ModelRef { key, level })
    }
}

#[cfg(test)]
mod tests {
    use super::{ModelKey, ModelRef};

    #[test]
    fn model_key_parse_splits_on_the_first_slash() {
        let k = ModelKey::parse("tama/m-1").expect("a valid key");
        assert_eq!(k.provider, "tama");
        assert_eq!(k.id, "m-1");
        // The FIRST `/` splits: model ids MAY contain `/` (the
        // documented rule — `resolve_composed_model`).
        let k = ModelKey::parse("a/b/c").expect("a valid key");
        assert_eq!(k.provider, "a");
        assert_eq!(k.id, "b/c");
    }

    #[test]
    fn model_key_parse_requires_a_slash() {
        assert_eq!(ModelKey::parse("noid"), None);
        // An empty provider is structurally a valid split (mirrors the old
        // `split_once('/')` — whether it names a real model is the catalog
        // lookup's job).
        let k = ModelKey::parse("/x").expect("empty provider parses");
        assert_eq!(k.provider, "");
        assert_eq!(k.id, "x");
    }

    #[test]
    fn model_key_new_round_trips_through_to_string() {
        let k = ModelKey::new("tama", "Qwen/Qwen3.8-27B");
        assert_eq!(k.to_string(), "tama/Qwen/Qwen3.8-27B");
        let parsed = ModelKey::parse(&k.to_string()).expect("round trip");
        assert_eq!(parsed, k);
    }

    #[test]
    fn model_ref_parse_strips_the_trailing_level() {
        let r = ModelRef::parse("tama/m-1:high").expect("a valid ref");
        assert_eq!(r.key, ModelKey::new("tama", "m-1"));
        assert_eq!(r.level.as_deref(), Some("high"));

        let r = ModelRef::parse("tama/m-1").expect("a valid ref");
        assert_eq!(r.key, ModelKey::new("tama", "m-1"));
        assert_eq!(r.level, None);
    }

    #[test]
    fn model_ref_parse_splits_on_the_last_colon() {
        // A model id may contain `:` — the level is always the TRAILING
        // segment (mirrors `rsplit_once(':')`).
        let r = ModelRef::parse("tama/m:1:high").expect("a valid ref");
        assert_eq!(r.key, ModelKey::new("tama", "m:1"));
        assert_eq!(r.level.as_deref(), Some("high"));
    }

    #[test]
    fn model_ref_parse_requires_a_valid_key() {
        // No `/` → the bare part is not a valid key → `None` (a
        // `"weird:id"` ref degrades to no key, same as a bare `"weird"`).
        assert_eq!(ModelRef::parse("weird"), None);
        assert_eq!(ModelRef::parse("weird:id"), None);
    }
}
