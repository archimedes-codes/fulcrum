//! JSON-RPC 2.0 (the MCP wire protocol's message shape): the request
//! build + the response / notification parse. Both transports (stdio's
//! newline-delimited lines, streamable-HTTP's JSON / SSE `data:` lines)
//! carry the SAME JSON-RPC 2.0 objects — this module is the shared seam.

use serde_json::{json, Value};

/// A JSON-RPC 2.0 request (a client → server `method` call with an `id` —
/// the server answers with a response carrying the SAME `id`).
#[derive(Debug, Clone, PartialEq)]
pub struct RpcRequest {
    pub id: u64,
    pub method: String,
    pub params: Value,
}

impl RpcRequest {
    /// The wire form (a single JSON object — the stdio transport appends
    /// a `\n`; the HTTP transport POSTs it as the body):
    /// `{"jsonrpc":"2.0","id":<id>,"method":<m>,"params":<p>}`.
    pub fn to_json(&self) -> Value {
        json!({
            "jsonrpc": "2.0",
            "id": self.id,
            "method": self.method,
            "params": self.params,
        })
    }
}

/// A JSON-RPC 2.0 error object (`error: { code, message, data? }`).
#[derive(Debug, Clone, PartialEq)]
pub struct SessionError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

/// A JSON-RPC 2.0 response (a server → client answer to a request:
/// `result` (success) OR `error` (failure), keyed by the request's `id`).
#[derive(Debug, Clone, PartialEq)]
pub struct RpcResponse {
    pub id: Value,
    pub result: Option<Value>,
    pub error: Option<SessionError>,
}

/// Parse a server line as a RESPONSE (a JSON object carrying an `id`):
/// - a valid JSON object with an `id` → `Ok(RpcResponse)` (`result` and/or
///   `error` extracted; an `error` object `{ code, message, data? }` →
///   `SessionError` — a non-object `error` is a parse error).
/// - a non-object / invalid JSON / a missing `id` (a notification) →
///   `Err` (the caller routes it to `parse_notification`).
pub fn parse_response(line: &str) -> Result<RpcResponse, String> {
    let v: Value = serde_json::from_str(line).map_err(|e| format!("not a JSON object: {e}"))?;
    let Some(map) = v.as_object() else {
        return Err("not a JSON object".to_string());
    };
    let Some(id) = map.get("id").cloned() else {
        return Err("no id (a notification, not a response)".to_string());
    };
    let error = match map.get("error") {
        Some(Value::Null) | None => None,
        Some(e) => {
            let em = e
                .as_object()
                .ok_or_else(|| "the error is not an object".to_string())?;
            let code = em
                .get("code")
                .and_then(Value::as_i64)
                .ok_or_else(|| "the error has no code".to_string())?;
            let message = em
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            Some(SessionError {
                code,
                message,
                data: em.get("data").cloned(),
            })
        }
    };
    Ok(RpcResponse {
        id,
        result: map.get("result").cloned(),
        error,
    })
}

/// Parse a server line as a NOTIFICATION (a JSON object with a `method`
/// and NO `id` — e.g. `notifications/initialized` answers, or a
/// `tools/list_changed` push): `Ok(method)`. A line with an `id` (a
/// response) or without a `method` → `Err`.
pub fn parse_notification(line: &str) -> Result<String, String> {
    let v: Value = serde_json::from_str(line).map_err(|e| format!("not a JSON object: {e}"))?;
    let Some(map) = v.as_object() else {
        return Err("not a JSON object".to_string());
    };
    if map.get("id").is_some() {
        return Err("has an id (a response, not a notification)".to_string());
    }
    let method = map
        .get("method")
        .and_then(Value::as_str)
        .ok_or_else(|| "no method".to_string())?;
    Ok(method.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_to_json_is_the_wire_shape() {
        let req = RpcRequest {
            id: 7,
            method: "tools/call".into(),
            params: json!({ "name": "echo", "arguments": { "x": 1 } }),
        };
        let v = req.to_json();
        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["id"], 7);
        assert_eq!(v["method"], "tools/call");
        assert_eq!(v["params"]["name"], "echo");
        assert_eq!(v["params"]["arguments"]["x"], 1);
    }

    #[test]
    fn parse_response_success() {
        let r = parse_response(
            r#"{"jsonrpc":"2.0","id":7,"result":{"content":[{"type":"text","text":"hi"}]}}"#,
        )
        .expect("a result response parses");
        assert_eq!(r.id, Value::from(7));
        assert!(r.error.is_none());
        assert_eq!(r.result.as_ref().unwrap()["content"][0]["text"], "hi");
    }

    #[test]
    fn parse_response_error_object() {
        let r = parse_response(
            r#"{"jsonrpc":"2.0","id":8,"error":{"code":-32601,"message":"Method not found","data":{"hint":"x"}}}"#,
        )
        .expect("an error response parses");
        assert_eq!(r.id, Value::from(8));
        assert!(r.result.is_none());
        let e = r.error.expect("the error is present");
        assert_eq!(e.code, -32601);
        assert_eq!(e.message, "Method not found");
        assert_eq!(e.data.as_ref().unwrap()["hint"], "x");
    }

    #[test]
    fn parse_response_a_notification_is_an_error() {
        assert!(
            parse_response(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#).is_err()
        );
    }

    #[test]
    fn parse_response_malformed_lines_are_errors() {
        assert!(parse_response("not json").is_err());
        assert!(parse_response("[1,2,3]").is_err()); // an array, not an object
        assert!(parse_response(r#"{"jsonrpc":"2.0","id":1,"error":"not-an-object"}"#).is_err());
        assert!(
            parse_response(r#"{"jsonrpc":"2.0","id":1,"error":{"message":"no code"}}"#).is_err()
        );
    }

    #[test]
    fn parse_notification_parses_a_notification_line() {
        assert_eq!(
            parse_notification(
                r#"{"jsonrpc":"2.0","method":"notifications/tools/list_changed","params":{}}"#
            )
            .expect("a notification parses"),
            "notifications/tools/list_changed"
        );
    }

    #[test]
    fn parse_notification_rejects_responses_and_methodless_lines() {
        assert!(parse_notification(r#"{"jsonrpc":"2.0","id":1,"result":{}}"#).is_err());
        assert!(parse_notification(r#"{"jsonrpc":"2.0"}"#).is_err()); // no method
    }
}
