//! Custom provider headers (issue #129): `$VAR` / `${VAR}` expansion against the
//! PROCESS environment in ONE SINGLE PASS (an expanded value is never
//! re-evaluated — the "no recursive evaluation" rule), plus the insecure-HTTP
//! gate (`http://` to a non-loopback host is refused unless `allowInsecureHttp`).
//! Validation is part of expansion: a header NAME that is not a valid header
//! token, or a VALUE containing CR/LF or non-visible ASCII, is SKIPPED with a
//! `debuglog` line — never propagated (reqwest's `RequestBuilder::header`
//! PANICS on an invalid value; the skip is the CRLF-injection guard).
//!
//! Why a single pass is the whole contract: a regex `replace_all` whose
//! replacement is re-scanned (or a loop that re-expands until stable) WOULD
//! evaluate recursively and silently dereference a variable the expanded value
//! merely MENTIONS — e.g. `A="$B"` plus `B=<secret>` turning `"$A"` into the
//! secret. Scanning left to right exactly once and appending each substitution
//! verbatim makes that impossible by construction, and the tests pin it.

use std::collections::BTreeMap;
use std::net::IpAddr;

/// Expand a whole header map. `None` in → `None` out is the caller's job (see
/// the call sites in `session/mod.rs`); this takes the present map and returns
/// the EXPANDED + VALIDATED one. Invalid names/values are skipped + debuglogged
/// so a malformed row degrades to "that one header is absent", never to a panic
/// and never to a whole provider being dropped.
pub fn expand_map(headers: &std::collections::HashMap<String, String>) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for (name, value) in headers {
        if !is_valid_header_name(name) {
            crate::agent::debuglog::log(&format!(
                "provider-auth: skipping custom header with an invalid name `{name}` \
                 (must be a non-empty header token)"
            ));
            continue;
        }
        let expanded = expand_value(value);
        if !is_valid_header_value(&expanded) {
            // The name is echoed, never the value — a header value is the secret
            // this whole module exists to move around.
            crate::agent::debuglog::log(&format!(
                "provider-auth: skipping custom header `{name}` (expanded value contains CR, LF \
                 or a non-visible-ASCII byte)"
            ));
            continue;
        }
        out.insert(name.clone(), expanded);
    }
    out
}

/// Expand ONE header value. `$$` escapes a literal `$` (documented; `$$` never
/// opens a name). `$NAME` / `${NAME}` read `std::env::var` — an UNSET variable
/// expands to `""`. A malformed name (the `${…}` braces do not balance, or the
/// name is empty) expands to the LITERAL text (never a panic, never a hang).
pub fn expand_value(value: &str) -> String {
    // One left-to-right pass; every substitution is appended VERBATIM and never
    // re-scanned (the module doc's single-pass rule).
    let bytes: Vec<char> = value.chars().collect();
    let mut out = String::with_capacity(value.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != '$' {
            out.push(bytes[i]);
            i += 1;
            continue;
        }
        // We are on a '$'.
        let Some(&next) = bytes.get(i + 1) else {
            // A trailing '$' opens nothing — literal.
            out.push('$');
            i += 1;
            continue;
        };
        if next == '$' {
            // `$$` is the documented escape for a literal `$`.
            out.push('$');
            i += 2;
            continue;
        }
        if next == '{' {
            // `${NAME}` — the closing brace is the FIRST one (no nesting); a
            // missing brace or an empty/invalid name leaves the text literal.
            match find_braced_name(&bytes[i + 2..]) {
                Some((name, consumed)) => {
                    out.push_str(&env_or_empty(&name));
                    i += 2 + consumed;
                }
                None => {
                    out.push('$');
                    i += 1;
                }
            }
            continue;
        }
        // Bare `$NAME` — the longest run of `[A-Za-z_][A-Za-z0-9_]*`. A leading
        // digit / punctuation is not a name, so `$1` stays literal.
        let (name, len) = read_bare_name(&bytes[i + 1..]);
        if name.is_empty() {
            out.push('$');
            i += 1;
        } else {
            out.push_str(&env_or_empty(&name));
            i += 1 + len;
        }
    }
    out
}

/// The name inside `${…}` plus how many chars it consumed (including the closing
/// brace). `None` when the braces do not balance or the name is not an env-var
/// name — the caller then emits the `'$'` literally.
fn find_braced_name(chars: &[char]) -> Option<(String, usize)> {
    let close = chars.iter().position(|&c| c == '}')?;
    let name: String = chars[..close].iter().collect();
    if !is_env_name(&name) {
        return None;
    }
    Some((name, close + 1))
}

/// The longest leading `[A-Za-z_][A-Za-z0-9_]*` run plus its length. An empty
/// name means the character after `$` opens nothing (`$1`, `$.foo`).
fn read_bare_name(chars: &[char]) -> (String, usize) {
    let Some(&first) = chars.first() else {
        return (String::new(), 0);
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return (String::new(), 0);
    }
    let mut end = 1;
    while end < chars.len() && (chars[end].is_ascii_alphanumeric() || chars[end] == '_') {
        end += 1;
    }
    (chars[..end].iter().collect(), end)
}

/// `true` when `name` is a legal shell-ish variable name.
fn is_env_name(name: &str) -> bool {
    let mut it = name.chars();
    let Some(first) = it.next() else { return false };
    (first.is_ascii_alphabetic() || first == '_')
        && it.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// `std::env::var` returns `Err` for an UNSET var AND for a non-Unicode value —
/// both are treated as unset (`""`), which is the documented behaviour.
fn env_or_empty(name: &str) -> String {
    std::env::var(name).unwrap_or_default()
}

/// A header name is a non-empty HTTP token (RFC 7230 `field-name` = 1*tchar).
/// Anything else (space, colon, CR, LF, a control byte, a non-ASCII byte) is
/// rejected rather than handed to reqwest, which would panic on it.
fn is_valid_header_name(name: &str) -> bool {
    !name.is_empty()
        && name.chars().all(|c| {
            c.is_ascii_alphanumeric()
                || matches!(
                    c,
                    '!' | '#'
                        | '$'
                        | '%'
                        | '&'
                        | '\''
                        | '*'
                        | '+'
                        | '-'
                        | '.'
                        | '^'
                        | '_'
                        | '`'
                        | '|'
                        | '~'
                )
        })
}

/// A header value must be visible ASCII (0x21..=0x7E) or a space/HTAB — the
/// reqwest-validity set minus the CR/LF that makes a request-splitting
/// injection possible. An empty value is legal HTTP and stays legal here.
fn is_valid_header_value(value: &str) -> bool {
    value
        .chars()
        .all(|c| c == '\t' || (c as u32) == 0x20 || ((c as u32) >= 0x21 && (c as u32) <= 0x7E))
}

/// `true` when `host` can only resolve to loopback addresses. Mirrors
/// `agent::web::net`'s discipline (a bare-IP `parse` first, then `lookup_host`):
/// `127.0.0.0/8` + `::1` are the entire loopback policy (ADR 0057) and are
/// exactly what `IpAddr::is_loopback()` means — no range table is re-derived,
/// and IPv4-mapped IPv6 (`::ffff:127.0.0.1`) is unwrapped exactly as
/// `net::is_blocked_ip` does.
///
/// A host that FAILS to resolve is NOT loopback → the gate refuses it
/// (fail-closed). `localhost` resolves to `127.0.0.1`/`::1` → allowed, matching
/// the ADR 0057 "local dev server access is essential" ruling.
async fn is_loopback_host(host: &str) -> bool {
    let host = host.trim_start_matches('[').trim_end_matches(']');
    if let Ok(ip) = host.parse::<IpAddr>() {
        return unwrap_mapped(ip).is_loopback();
    }
    // Port 0: only the addresses matter. An unresolvable name is `false`.
    let Ok(addrs) = tokio::net::lookup_host((host, 0)).await else {
        return false;
    };
    // `all` over a NON-EMPTY set: an empty answer means "nothing is reachable",
    // which must not read as loopback, so the empty case is refused explicitly.
    let mut saw = false;
    for sa in addrs {
        saw = true;
        if !unwrap_mapped(sa.ip()).is_loopback() {
            return false;
        }
    }
    saw
}

/// Unwrap an IPv4-mapped IPv6 address, exactly as `net::is_blocked_ip` does.
fn unwrap_mapped(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(v6),
        },
        other => other,
    }
}

/// The insecure-HTTP gate on a provider base URL. `Err` carries the DESCRIPTIVE
/// error (naming `allowInsecureHttp`) unless the scheme is https, the host is
/// loopback, or `allow_insecure_http` is true.
///
/// The order is deliberate: `https` short-circuits before any DNS work, and the
/// explicit opt-out is honoured before the loopback probe, so an operator who
/// set the flag never pays a lookup for a decision already made.
pub async fn validate_base_url(base_url: &str, allow_insecure_http: bool) -> Result<(), String> {
    let url = url::Url::parse(base_url).map_err(|_| format!("invalid base URL {base_url}"))?;
    let scheme = url.scheme();
    if scheme == "https" {
        return Ok(());
    }
    if allow_insecure_http {
        // Documented: the user accepted the risk.
        return Ok(());
    }
    if scheme != "http" {
        // Fail-closed: the providers speak http(s) only (matches
        // `NetError::DeniedScheme`'s spirit).
        return Err(format!(
            "provider base URL {base_url} uses the `{scheme}` scheme; only http:// and https:// \
             are supported (set allowInsecureHttp to permit plain http://)"
        ));
    }
    let Some(host) = url.host_str() else {
        return Err(format!(
            "provider base URL {base_url} uses http:// with no host (insecure); \
             set allowInsecureHttp or use https"
        ));
    };
    if is_loopback_host(host).await {
        return Ok(());
    }
    Err(format!(
        "provider base URL {base_url} uses http:// to a non-loopback host (insecure); \
         set allowInsecureHttp or use https"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::env_lock;

    /// `PERPLEXITY_API_KEY` restore guard (the `commands/settings.rs` pattern —
    /// a copy of the private `RestoreEnv` in `agent/web/config.rs`'s test
    /// module; the restore happens even when an assertion panics).
    struct RestoreEnv(&'static str, Option<std::ffi::OsString>);
    impl Drop for RestoreEnv {
        fn drop(&mut self) {
            match self.1.take() {
                // SAFETY: the `ENV_LOCK` is still held at drop time (this guard
                // is declared after the `_lock` guard and outlives it in
                // reverse); no other thread mutates the env concurrently.
                Some(v) => unsafe { std::env::set_var(self.0, v) },
                None => unsafe { std::env::remove_var(self.0) },
            }
        }
    }

    /// Set `key` to `value` for the rest of the test, returning the guard that
    /// restores whatever was there. Caller must hold `env_lock()`.
    fn set_env(key: &'static str, value: &str) -> RestoreEnv {
        let prev = std::env::var_os(key);
        // SAFETY: `ENV_LOCK` is held by caller.
        unsafe { std::env::set_var(key, value) };
        RestoreEnv(key, prev)
    }

    /// Unset `key` for the rest of the test, returning the restore guard.
    /// Caller must hold `env_lock()`.
    fn remove_env(key: &'static str) -> RestoreEnv {
        let prev = std::env::var_os(key);
        // SAFETY: `ENV_LOCK` is held by caller.
        unsafe { std::env::remove_var(key) };
        RestoreEnv(key, prev)
    }

    #[test]
    fn expands_dollar_var_from_the_environment() {
        let _lock = env_lock();
        let _g = set_env("LITELLM_TEST_HEADER_A", "tok-1");
        assert_eq!(expand_value("$LITELLM_TEST_HEADER_A"), "tok-1");
    }

    #[test]
    fn expands_braced_var_from_the_environment() {
        let _lock = env_lock();
        let _g = set_env("LITELLM_TEST_HEADER_A", "tok-1");
        assert_eq!(expand_value("${LITELLM_TEST_HEADER_A}"), "tok-1");
        // Embedded in a longer template, both spellings.
        assert_eq!(
            expand_value("Bearer ${LITELLM_TEST_HEADER_A}; $LITELLM_TEST_HEADER_A"),
            "Bearer tok-1; tok-1"
        );
    }

    #[test]
    fn unset_variables_expand_to_empty() {
        let _lock = env_lock();
        let _g = remove_env("LITELLM_TEST_HEADER_UNSET");
        assert_eq!(expand_value("$LITELLM_TEST_HEADER_UNSET"), "");
        assert_eq!(
            expand_value("pre-${LITELLM_TEST_HEADER_UNSET}-post"),
            "pre--post"
        );
    }

    #[test]
    fn dollar_dollar_is_a_literal_dollar() {
        // `$$` never opens a name, so no env work happens at all.
        assert_eq!(expand_value("$$"), "$");
        assert_eq!(expand_value("a$$b"), "a$b");
        let _lock = env_lock();
        let _g = set_env("LITELLM_TEST_HEADER_A", "tok-1");
        // `$$PATH` is a literal `$` followed by the name's text ONLY because the
        // escape consumed the first '$' — the name after it is NOT expanded.
        assert_eq!(
            expand_value("$$LITELLM_TEST_HEADER_A"),
            "$LITELLM_TEST_HEADER_A"
        );
    }

    #[test]
    fn expanded_values_are_never_rescanned_single_pass() {
        let _lock = env_lock();
        let _a = set_env("LITELLM_TEST_T2A_VAR_A", "$LITELLM_TEST_T2A_VAR_B");
        let _b = set_env("LITELLM_TEST_T2A_VAR_B", "secret");
        assert_eq!(
            expand_value("$LITELLM_TEST_T2A_VAR_A"),
            "$LITELLM_TEST_T2A_VAR_B"
        );
        assert!(
            !expand_value("$LITELLM_TEST_T2A_VAR_A").contains("secret"),
            "a single pass must never dereference the variable the value merely mentions"
        );
    }

    #[test]
    fn expanded_braced_values_are_never_rescanned_single_pass() {
        let _lock = env_lock();
        let _a = set_env("LITELLM_TEST_T2A_VAR_A", "${LITELLM_TEST_T2A_VAR_B}");
        let _b = set_env("LITELLM_TEST_T2A_VAR_B", "secret");
        assert_eq!(
            expand_value("${LITELLM_TEST_T2A_VAR_A}"),
            "${LITELLM_TEST_T2A_VAR_B}"
        );
        assert!(
            !expand_value("${LITELLM_TEST_T2A_VAR_A}").contains("secret"),
            "a single pass must never dereference the variable the value merely mentions"
        );
    }

    #[test]
    fn malformed_braces_stay_literal() {
        // Documented, pinned: a malformed name is the LITERAL text — never a
        // panic, never a hang, never a partial substitution.
        assert_eq!(expand_value("${"), "${");
        assert_eq!(expand_value("${}"), "${}");
        assert_eq!(expand_value("${"), "${");
        assert_eq!(expand_value("${1BAD}"), "${1BAD}");
        assert_eq!(expand_value("a${b"), "a${b");
        // A trailing '$' opens nothing either.
        assert_eq!(expand_value("tail$"), "tail$");
        assert_eq!(expand_value("$1digit"), "$1digit");
    }

    #[test]
    fn a_header_with_an_invalid_name_is_skipped() {
        let _lock = env_lock();
        let mut map = std::collections::HashMap::new();
        map.insert("x good".to_string(), "v".to_string());
        map.insert("x: weird".to_string(), "v".to_string());
        map.insert(String::new(), "v".to_string());
        map.insert("x-good".to_string(), "kept".to_string());
        let out = expand_map(&map);
        assert_eq!(out.len(), 1, "{out:?}");
        assert_eq!(out.get("x-good").map(String::as_str), Some("kept"));
    }

    #[test]
    fn a_header_with_an_invalid_value_is_skipped() {
        let _lock = env_lock();
        let mut map = std::collections::HashMap::new();
        // CRLF injection (the request-splitting shape).
        map.insert("x-cr".to_string(), "a\r\nX-Injected: 1".to_string());
        map.insert("x-lf".to_string(), "a\nb".to_string());
        // A non-visible-ASCII byte (NUL, DEL, a non-ASCII codepoint).
        map.insert("x-nul".to_string(), "a\0b".to_string());
        map.insert("x-del".to_string(), "a\u{7F}b".to_string());
        map.insert("x-utf8".to_string(), "café".to_string());
        // The good siblings survive (space/HTAB are legal in a value).
        map.insert("x-ok".to_string(), "a b".to_string());
        map.insert("x-empty".to_string(), String::new());
        let out = expand_map(&map);
        assert_eq!(out.len(), 2, "{out:?}");
        assert_eq!(out.get("x-ok").map(String::as_str), Some("a b"));
        assert_eq!(out.get("x-empty").map(String::as_str), Some(""));
        for bad in ["x-cr", "x-lf", "x-nul", "x-del", "x-utf8"] {
            assert!(!out.contains_key(bad), "{bad} must be skipped");
        }
    }

    #[test]
    fn a_valid_expansion_is_what_reaches_the_map() {
        let _lock = env_lock();
        let _g = set_env("LITELLM_TEST_HEADER_A", "tok-1");
        let mut map = std::collections::HashMap::new();
        map.insert(
            "x-token".to_string(),
            "Bearer $LITELLM_TEST_HEADER_A".to_string(),
        );
        let out = expand_map(&map);
        assert_eq!(out.get("x-token").map(String::as_str), Some("Bearer tok-1"));
    }

    #[tokio::test]
    async fn http_to_loopback_is_allowed() {
        for url in [
            "http://127.0.0.1/v1",
            "http://127.0.0.1:4000/v1",
            "http://[::1]:8080/v1",
            "http://localhost:8080/v1",
            // IPv4-mapped IPv6 unwraps to loopback (net::is_blocked_ip's rule).
            "http://[::ffff:127.0.0.1]/v1",
        ] {
            assert!(
                validate_base_url(url, false).await.is_ok(),
                "{url} is loopback and must always be allowed (ADR 0057)"
            );
        }
    }

    #[tokio::test]
    async fn http_to_a_non_loopback_host_is_rejected() {
        for url in ["http://192.168.1.5/v1", "http://example.com/v1"] {
            let err = validate_base_url(url, false)
                .await
                .expect_err("{url} is a plain-HTTP non-loopback base and must be refused");
            assert!(
                err.contains("allowInsecureHttp"),
                "the error must name the opt-out: {err}"
            );
            assert!(err.contains(url), "the error must name the URL: {err}");
        }
    }

    #[tokio::test]
    async fn https_to_any_host_is_allowed() {
        assert!(validate_base_url("https://example.com/v1", false)
            .await
            .is_ok());
        // Even a private address is fine over https (the gate is about plaintext).
        assert!(validate_base_url("https://192.168.1.5/v1", false)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn allow_insecure_http_lifts_the_gate() {
        assert!(validate_base_url("http://192.168.1.5/v1", true)
            .await
            .is_ok());
        assert!(validate_base_url("http://example.com/v1", true)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn an_unresolvable_host_is_refused() {
        // `.invalid` is reserved and never resolves (RFC 2606) — fail-closed.
        let err = validate_base_url("http://no-such-host.invalid/v1", false)
            .await
            .expect_err("a host that cannot be resolved must not read as loopback");
        assert!(err.contains("allowInsecureHttp"), "{err}");
    }

    #[tokio::test]
    async fn a_non_http_scheme_is_refused_and_a_malformed_url_is_reported() {
        let err = validate_base_url("ftp://127.0.0.1/v1", false)
            .await
            .expect_err("only http(s) is supported");
        assert!(err.contains("ftp"), "{err}");
        let err = validate_base_url("not a url", false)
            .await
            .expect_err("an unparseable base URL is an error, not a panic");
        assert!(err.contains("invalid base URL"), "{err}");
    }

    #[test]
    fn the_loopback_helper_predicate_agrees_with_the_std_one() {
        // The pure half of `is_loopback_host`: bare-IP parse + mapped unwrap.
        for (raw, want) in [
            ("127.0.0.1", true),
            ("127.1.2.3", true),
            ("::1", true),
            ("::ffff:127.0.0.1", true),
            ("[::1]", true),
            ("10.0.0.5", false),
            ("192.168.1.5", false),
            ("93.184.216.34", false),
        ] {
            let ip: IpAddr = raw
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse()
                .expect("test literal is an IP");
            assert_eq!(unwrap_mapped(ip).is_loopback(), want, "{raw}");
        }
    }

    #[tokio::test]
    async fn is_loopback_host_covers_bare_ips_and_names() {
        assert!(is_loopback_host("127.0.0.1").await);
        assert!(is_loopback_host("[::1]").await);
        assert!(is_loopback_host("localhost").await);
        assert!(!is_loopback_host("10.0.0.5").await);
        assert!(!is_loopback_host("no-such-host.invalid").await);
    }
}
