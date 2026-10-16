use futures_util::StreamExt;
use ipnet::{Ipv4Net, Ipv6Net};
use reqwest::header::{HeaderMap, CONTENT_LENGTH, CONTENT_TYPE, LOCATION};
use reqwest::Method;
use std::net::{IpAddr, SocketAddr};
use std::sync::LazyLock;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use url::Url;

pub const MAX_BODY_BYTES: usize = 5 * 1024 * 1024; // 5 MB
pub const MAX_REDIRECTS: usize = 5;
pub const OVERALL_TIMEOUT: Duration = Duration::from_secs(15);
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

static IPV4_BLOCKED: LazyLock<Vec<Ipv4Net>> = LazyLock::new(|| {
    vec![
        "0.0.0.0/8".parse().unwrap(),
        "10.0.0.0/8".parse().unwrap(),
        "100.64.0.0/10".parse().unwrap(),
        "169.254.0.0/16".parse().unwrap(),
        "172.16.0.0/12".parse().unwrap(),
        "192.0.0.0/24".parse().unwrap(),
        "192.168.0.0/16".parse().unwrap(),
        "198.18.0.0/15".parse().unwrap(),
        "224.0.0.0/4".parse().unwrap(),
        "240.0.0.0/4".parse().unwrap(),
    ]
});

static IPV6_BLOCKED: LazyLock<Vec<Ipv6Net>> = LazyLock::new(|| {
    vec![
        "::/128".parse().unwrap(),
        "fc00::/7".parse().unwrap(),
        "fe80::/10".parse().unwrap(),
        "ff00::/8".parse().unwrap(),
    ]
});

#[derive(Debug, thiserror::Error)]
pub enum NetError {
    #[error("invalid URL: {0}")]
    InvalidUrl(String),
    #[error("denied scheme `{0}`: only http and https are allowed")]
    DeniedScheme(String),
    #[error("cannot resolve host `{0}`")]
    Unresolved(String),
    #[error("blocked IP address `{0}`: SSRF protection")]
    BlockedAddress(String),
    #[error("too many redirects (max 5)")]
    TooManyRedirects,
    #[error("redirect missing location header")]
    MissingRedirectLocation,
    #[error("response body exceeded limit of 5 MB")]
    TooLarge,
    #[error("request timed out")]
    Timeout,
    #[error("operation cancelled")]
    Cancelled,
    #[error("network error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub struct FetchOutcome {
    pub status: u16,
    pub final_url: Url,
    pub content_type: String,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct FetchOpts {
    pub method: Method,
    pub headers: HeaderMap,
    pub body: Option<Vec<u8>>,
    pub proxy: Option<String>,
}

impl Default for FetchOpts {
    fn default() -> Self {
        Self {
            method: Method::GET,
            headers: HeaderMap::new(),
            body: None,
            proxy: None,
        }
    }
}

/// Checks whether an IP address is in a blocked private/reserved range.
///
/// Note: loopback (`127.0.0.0/8` and `::1`) is EXPLICITLY allowed (ADR 0057:
/// local dev server access is essential for a coding agent).
pub fn is_blocked_ip(ip: IpAddr) -> bool {
    // Unwrap IPv4-mapped IPv6 address (e.g. ::ffff:127.0.0.1 or ::ffff:10.0.0.1)
    let ip = match ip {
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                IpAddr::V4(v4)
            } else {
                IpAddr::V6(v6)
            }
        }
        IpAddr::V4(v4) => IpAddr::V4(v4),
    };

    match ip {
        IpAddr::V4(v4) => IPV4_BLOCKED.iter().any(|net| net.contains(&v4)),
        IpAddr::V6(v6) => IPV6_BLOCKED.iter().any(|net| net.contains(&v6)),
    }
}

/// Strip sensitive headers on cross-origin redirects.
fn strip_cross_origin_headers(headers: &mut HeaderMap) {
    headers.remove("authorization");
    headers.remove("cookie");
    headers.remove("x-subscription-token");
}

fn is_cross_origin(a: &Url, b: &Url) -> bool {
    a.scheme() != b.scheme()
        || a.host_str() != b.host_str()
        || a.port_or_known_default() != b.port_or_known_default()
}

/// Resolve host to socket addresses and verify NONE of the addresses are blocked.
pub async fn resolve_and_verify_addrs(host: &str, port: u16) -> Result<Vec<SocketAddr>, NetError> {
    // If host is an IPv6 literal with brackets, strip them for lookup_host if needed
    let clean_host = host.trim_start_matches('[').trim_end_matches(']');

    // First try parsing as bare IP directly
    if let Ok(ip) = clean_host.parse::<IpAddr>() {
        if is_blocked_ip(ip) {
            return Err(NetError::BlockedAddress(ip.to_string()));
        }
        return Ok(vec![SocketAddr::new(ip, port)]);
    }

    let addrs: Vec<SocketAddr> = tokio::net::lookup_host((clean_host, port))
        .await
        .map_err(|_| NetError::Unresolved(host.to_string()))?
        .collect();

    if addrs.is_empty() {
        return Err(NetError::Unresolved(host.to_string()));
    }

    for addr in &addrs {
        if is_blocked_ip(addr.ip()) {
            return Err(NetError::BlockedAddress(addr.ip().to_string()));
        }
    }

    Ok(addrs)
}

/// Outbound fetch guarded against SSRF and DNS rebinding.
///
/// Features:
/// 1. Only http and https schemes.
/// 2. Pre-resolves host and verifies all resolved IPs against the SSRF blocklist.
/// 3. Pins resolved addresses into reqwest per-request to defeat DNS rebinding.
/// 4. Handles up to 5 redirects manually, re-checking SSRF at each hop.
/// 5. Strips credentials and downgrades method appropriately across origins.
/// 6. Streams response body and enforces a strict 5 MB limit.
/// 7. 15s overall timeout, 10s connect timeout, integrated with cancellation token.
pub async fn safe_fetch(
    url_str: &str,
    opts: &FetchOpts,
    cancel: &CancellationToken,
) -> Result<FetchOutcome, NetError> {
    tokio::select! {
        _ = cancel.cancelled() => Err(NetError::Cancelled),
        _ = tokio::time::sleep(OVERALL_TIMEOUT) => Err(NetError::Timeout),
        res = safe_fetch_inner(url_str, opts, cancel) => res,
    }
}

async fn safe_fetch_inner(
    url_str: &str,
    opts: &FetchOpts,
    cancel: &CancellationToken,
) -> Result<FetchOutcome, NetError> {
    let mut current_url = Url::parse(url_str).map_err(|e| NetError::InvalidUrl(e.to_string()))?;
    let mut current_method = opts.method.clone();
    let mut current_headers = opts.headers.clone();
    let mut current_body = opts.body.clone();

    // If proxy is given, verify it as well
    let verified_proxy = if let Some(ref proxy_str) = opts.proxy {
        let proxy_url = Url::parse(proxy_str).map_err(|e| NetError::InvalidUrl(e.to_string()))?;
        if proxy_url.scheme() != "http" && proxy_url.scheme() != "https" {
            return Err(NetError::DeniedScheme(proxy_url.scheme().to_string()));
        }
        let proxy_host = proxy_url
            .host_str()
            .ok_or_else(|| NetError::InvalidUrl("proxy missing host".to_string()))?;
        let proxy_port =
            proxy_url
                .port_or_known_default()
                .unwrap_or(if proxy_url.scheme() == "https" {
                    443
                } else {
                    80
                });
        let proxy_addrs = resolve_and_verify_addrs(proxy_host, proxy_port).await?;
        let clean_proxy_host = proxy_host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_string();
        Some((clean_proxy_host, proxy_addrs))
    } else {
        None
    };

    let mut redirects = 0;

    loop {
        if cancel.is_cancelled() {
            return Err(NetError::Cancelled);
        }

        if current_url.scheme() != "http" && current_url.scheme() != "https" {
            return Err(NetError::DeniedScheme(current_url.scheme().to_string()));
        }

        let host = current_url
            .host_str()
            .ok_or_else(|| NetError::InvalidUrl("missing host".to_string()))?;
        let port =
            current_url
                .port_or_known_default()
                .unwrap_or(if current_url.scheme() == "https" {
                    443
                } else {
                    80
                });

        let verified_addrs = resolve_and_verify_addrs(host, port).await?;

        let clean_host = host.trim_start_matches('[').trim_end_matches(']');
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(CONNECT_TIMEOUT)
            .resolve_to_addrs(clean_host, &verified_addrs);

        if let Some(ref proxy_str) = opts.proxy {
            builder = builder.proxy(reqwest::Proxy::all(proxy_str)?);
            if let Some((clean_proxy_host, proxy_addrs)) = &verified_proxy {
                builder = builder.resolve_to_addrs(clean_proxy_host, proxy_addrs);
            }
        } else {
            builder = builder.no_proxy();
        }

        let client = builder.build()?;

        let mut req = client.request(current_method.clone(), current_url.clone());
        req = req.headers(current_headers.clone());
        if let Some(ref body) = current_body {
            req = req.body(body.clone());
        }

        let resp = req.send().await?;
        let status = resp.status();

        if status.is_redirection() {
            if redirects >= MAX_REDIRECTS {
                return Err(NetError::TooManyRedirects);
            }

            let loc_header = resp
                .headers()
                .get(LOCATION)
                .ok_or(NetError::MissingRedirectLocation)?
                .to_str()
                .map_err(|_| NetError::MissingRedirectLocation)?;

            let next_url = current_url
                .join(loc_header)
                .map_err(|e| NetError::InvalidUrl(e.to_string()))?;

            let cross = is_cross_origin(&current_url, &next_url);
            if cross {
                strip_cross_origin_headers(&mut current_headers);
                // 307/308 keep method but never carry body cross-origin
                if status == reqwest::StatusCode::TEMPORARY_REDIRECT
                    || status == reqwest::StatusCode::PERMANENT_REDIRECT
                {
                    current_body = None;
                    current_headers.remove(CONTENT_TYPE);
                    current_headers.remove(CONTENT_LENGTH);
                }
            }

            // 301, 302, 303 downgrade to GET and drop body
            if status == reqwest::StatusCode::MOVED_PERMANENTLY
                || status == reqwest::StatusCode::FOUND
                || status == reqwest::StatusCode::SEE_OTHER
            {
                current_method = Method::GET;
                current_body = None;
                current_headers.remove(CONTENT_TYPE);
                current_headers.remove(CONTENT_LENGTH);
            }

            current_url = next_url;
            redirects += 1;
            continue;
        }

        // Check Content-Length if present
        if let Some(cl_val) = resp.headers().get(CONTENT_LENGTH) {
            if let Ok(cl_str) = cl_val.to_str() {
                if let Ok(cl) = cl_str.parse::<usize>() {
                    if cl > MAX_BODY_BYTES {
                        return Err(NetError::TooLarge);
                    }
                }
            }
        }

        let content_type = resp
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        let status_code = status.as_u16();

        // Stream body with 5 MB cap
        let mut stream = resp.bytes_stream();
        let mut body = Vec::new();

        while let Some(chunk_res) = stream.next().await {
            if cancel.is_cancelled() {
                return Err(NetError::Cancelled);
            }
            let chunk = chunk_res?;
            if body.len() + chunk.len() > MAX_BODY_BYTES {
                return Err(NetError::TooLarge);
            }
            body.extend_from_slice(&chunk);
        }

        return Ok(FetchOutcome {
            status: status_code,
            final_url: current_url,
            content_type,
            body,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::HeaderValue;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn test_is_blocked_ip_blocks_private_and_reserved() {
        // IPv4 blocked
        assert!(is_blocked_ip("10.0.0.1".parse().unwrap()));
        assert!(is_blocked_ip("10.255.255.255".parse().unwrap()));
        assert!(is_blocked_ip("172.16.0.1".parse().unwrap()));
        assert!(is_blocked_ip("172.31.255.255".parse().unwrap()));
        assert!(is_blocked_ip("192.168.0.1".parse().unwrap()));
        assert!(is_blocked_ip("192.168.255.255".parse().unwrap()));
        assert!(is_blocked_ip("169.254.169.254".parse().unwrap()));
        assert!(is_blocked_ip("100.64.0.1".parse().unwrap()));
        assert!(is_blocked_ip("0.0.0.0".parse().unwrap()));
        assert!(is_blocked_ip("192.0.0.1".parse().unwrap()));
        assert!(is_blocked_ip("198.18.0.1".parse().unwrap()));
        assert!(is_blocked_ip("224.0.0.1".parse().unwrap()));
        assert!(is_blocked_ip("240.0.0.1".parse().unwrap()));
        assert!(is_blocked_ip("255.255.255.255".parse().unwrap()));

        // IPv6 blocked
        assert!(is_blocked_ip("::".parse().unwrap()));
        assert!(is_blocked_ip("fc00::1".parse().unwrap()));
        assert!(is_blocked_ip("fe80::1".parse().unwrap()));
        assert!(is_blocked_ip("ff02::1".parse().unwrap()));

        // IPv4-mapped IPv6 blocked
        assert!(is_blocked_ip("::ffff:10.0.0.1".parse().unwrap()));
        assert!(is_blocked_ip("::ffff:169.254.169.254".parse().unwrap()));
        assert!(is_blocked_ip("::ffff:192.168.1.1".parse().unwrap()));
    }

    #[test]
    fn test_is_blocked_ip_allows_loopback_and_public() {
        // Loopback allowed (ADR 0057)
        assert!(!is_blocked_ip("127.0.0.1".parse().unwrap()));
        assert!(!is_blocked_ip("127.255.255.254".parse().unwrap()));
        assert!(!is_blocked_ip("::1".parse().unwrap()));
        assert!(!is_blocked_ip("::ffff:127.0.0.1".parse().unwrap()));
        assert!(!is_blocked_ip("::ffff:7f00:1".parse().unwrap()));

        // Public allowed
        assert!(!is_blocked_ip("8.8.8.8".parse().unwrap()));
        assert!(!is_blocked_ip("1.1.1.1".parse().unwrap()));
        assert!(!is_blocked_ip("172.32.0.1".parse().unwrap()));
        assert!(!is_blocked_ip("2607:f8b0:4005:805::200e".parse().unwrap()));
        assert!(!is_blocked_ip("::ffff:8.8.8.8".parse().unwrap()));
    }

    #[tokio::test]
    async fn test_safe_fetch_denies_schemes() {
        let cancel = CancellationToken::new();
        let opts = FetchOpts::default();

        let res = safe_fetch("file:///etc/passwd", &opts, &cancel).await;
        match res {
            Err(NetError::DeniedScheme(s)) => assert_eq!(s, "file"),
            other => panic!("expected DeniedScheme, got {other:?}"),
        }

        let res = safe_fetch("ftp://example.com/test", &opts, &cancel).await;
        match res {
            Err(NetError::DeniedScheme(s)) => assert_eq!(s, "ftp"),
            other => panic!("expected DeniedScheme, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_safe_fetch_blocks_private_ip() {
        let cancel = CancellationToken::new();
        let opts = FetchOpts::default();

        let res = safe_fetch("http://10.0.0.1/secret", &opts, &cancel).await;
        match res {
            Err(NetError::BlockedAddress(addr)) => assert_eq!(addr, "10.0.0.1"),
            other => panic!("expected BlockedAddress, got {other:?}"),
        }

        let res = safe_fetch("http://169.254.169.254/latest/meta-data", &opts, &cancel).await;
        match res {
            Err(NetError::BlockedAddress(addr)) => assert_eq!(addr, "169.254.169.254"),
            other => panic!("expected BlockedAddress, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_safe_fetch_success_and_cancellation() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/hello"))
            .respond_with(ResponseTemplate::new(200).set_body_string("hello world"))
            .mount(&server)
            .await;

        let cancel = CancellationToken::new();
        let opts = FetchOpts::default();
        let url = format!("{}/hello", server.uri());

        let outcome = safe_fetch(&url, &opts, &cancel).await.unwrap();
        assert_eq!(outcome.status, 200);
        assert_eq!(String::from_utf8(outcome.body).unwrap(), "hello world");

        // Cancelled before execution
        let pre_cancelled = CancellationToken::new();
        pre_cancelled.cancel();
        let err = safe_fetch(&url, &opts, &pre_cancelled).await.unwrap_err();
        match err {
            NetError::Cancelled => {}
            other => panic!("expected Cancelled, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_safe_fetch_redirect_and_cross_origin_header_stripping() {
        let server1 = MockServer::start().await;
        let server2 = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/redirect"))
            .respond_with(
                ResponseTemplate::new(302)
                    .insert_header("Location", format!("{}/dest", server2.uri())),
            )
            .mount(&server1)
            .await;

        // Server2 should NOT receive the Authorization header because it's a cross-origin redirect
        Mock::given(method("GET"))
            .and(path("/dest"))
            .respond_with(ResponseTemplate::new(200).set_body_string("reached destination"))
            .mount(&server2)
            .await;

        let mut headers = HeaderMap::new();
        headers.insert(
            "Authorization",
            HeaderValue::from_static("Bearer secret-token"),
        );
        headers.insert(
            "X-Subscription-Token",
            HeaderValue::from_static("api-key-123"),
        );
        let opts = FetchOpts {
            method: Method::POST,
            headers,
            body: Some(b"post body".to_vec()),
            proxy: None,
        };

        let cancel = CancellationToken::new();
        let url = format!("{}/redirect", server1.uri());
        let outcome = safe_fetch(&url, &opts, &cancel).await.unwrap();

        assert_eq!(outcome.status, 200);
        assert_eq!(
            outcome.final_url.as_str(),
            &format!("{}/dest", server2.uri())
        );
        assert_eq!(
            String::from_utf8(outcome.body).unwrap(),
            "reached destination"
        );

        // Verify requests received at server2 didn't have Authorization
        let requests = server2.received_requests().await.unwrap();
        assert_eq!(requests.len(), 1);
        let req = &requests[0];
        assert_eq!(req.method, "GET"); // 302 downgraded POST to GET
        assert!(req.headers.get("authorization").is_none());
        assert!(req.headers.get("x-subscription-token").is_none());
        assert!(req.body.is_empty()); // Body was dropped
    }

    #[tokio::test]
    async fn test_safe_fetch_redirect_limit() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/loop"))
            .respond_with(ResponseTemplate::new(302).insert_header("Location", "/loop"))
            .mount(&server)
            .await;

        let cancel = CancellationToken::new();
        let opts = FetchOpts::default();
        let url = format!("{}/loop", server.uri());

        let res = safe_fetch(&url, &opts, &cancel).await;
        match res {
            Err(NetError::TooManyRedirects) => {}
            other => panic!("expected TooManyRedirects, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_safe_fetch_proxy_blocks_private_ip() {
        let cancel = CancellationToken::new();
        let opts = FetchOpts {
            proxy: Some("http://10.0.0.1:8080".into()),
            ..Default::default()
        };
        let res = safe_fetch("http://127.0.0.1/test", &opts, &cancel).await;
        match res {
            Err(NetError::BlockedAddress(addr)) => assert_eq!(addr, "10.0.0.1"),
            other => panic!("expected BlockedAddress, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_safe_fetch_max_body_bytes() {
        let server = MockServer::start().await;
        // Body larger than 5 MB
        let big_body = vec![b'a'; MAX_BODY_BYTES + 10];
        Mock::given(method("GET"))
            .and(path("/toolarge"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(big_body))
            .mount(&server)
            .await;

        let cancel = CancellationToken::new();
        let opts = FetchOpts::default();
        let url = format!("{}/toolarge", server.uri());

        let res = safe_fetch(&url, &opts, &cancel).await;
        match res {
            Err(NetError::TooLarge) => {}
            other => panic!("expected TooLarge, got {other:?}"),
        }
    }
}
