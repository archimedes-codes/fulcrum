use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionId(String);

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RequestId(String);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct GateKey {
    pub session: SessionId,
    pub request: RequestId,
}

impl SessionId {
    pub fn mint() -> Self {
        Self(format!("arch_{}", uuid::Uuid::new_v4()))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn into_string(self) -> String {
        self.0
    }
}

impl RequestId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn with_suffix(&self, suffix: &str) -> Self {
        Self(format!("{}{}", self.0, suffix))
    }
}

impl GateKey {
    pub fn new(session: impl Into<SessionId>, request: impl Into<RequestId>) -> Self {
        Self {
            session: session.into(),
            request: request.into(),
        }
    }
    pub fn to_string_key(&self) -> String {
        format!("{}/{}", self.session, self.request)
    }
}

impl From<String> for SessionId {
    fn from(s: String) -> Self {
        Self(s)
    }
}
impl From<&str> for SessionId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}
impl AsRef<str> for SessionId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for RequestId {
    fn from(s: String) -> Self {
        Self(s)
    }
}
impl From<&str> for RequestId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}
impl AsRef<str> for RequestId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_id_mint() {
        let id1 = SessionId::mint();
        let id2 = SessionId::mint();
        assert!(id1.as_str().starts_with("arch_"));
        assert_ne!(id1, id2);
        assert_eq!(id1.as_str(), id1.as_ref());
    }

    #[test]
    fn test_session_id_conversions_and_display() {
        let s = "arch_12345";
        let from_str = SessionId::from(s);
        let from_string = SessionId::from(s.to_string());
        assert_eq!(from_str, from_string);
        assert_eq!(from_str.to_string(), s);
        assert_eq!(format!("{}", from_str), s);
        assert_eq!(from_str.into_string(), s);
    }

    #[test]
    fn test_session_id_serde_roundtrip() {
        let id = SessionId::from("arch_test_session");
        let json = serde_json::to_string(&id).expect("serialize");
        assert_eq!(json, "\"arch_test_session\"");
        let deserialized: SessionId = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(id, deserialized);
    }

    #[test]
    fn test_request_id_conversions_display_and_suffix() {
        let req1 = RequestId::new("req_1");
        assert_eq!(req1.as_str(), "req_1");
        assert_eq!(req1.as_ref(), "req_1");
        assert_eq!(format!("{}", req1), "req_1");

        let from_str = RequestId::from("req_2");
        let from_string = RequestId::from("req_2".to_string());
        assert_eq!(from_str, from_string);

        let suffixed = req1.with_suffix("_sub");
        assert_eq!(suffixed.as_str(), "req_1_sub");
    }

    #[test]
    fn test_request_id_serde_roundtrip() {
        let req = RequestId::from("req_abc");
        let json = serde_json::to_string(&req).expect("serialize");
        assert_eq!(json, "\"req_abc\"");
        let deserialized: RequestId = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(req, deserialized);
    }

    #[test]
    fn test_gate_key() {
        let session = SessionId::from("arch_sess_1");
        let request = RequestId::from("req_1");
        let key = GateKey::new(session.clone(), request.clone());
        assert_eq!(key.session, session);
        assert_eq!(key.request, request);
        assert_eq!(key.to_string_key(), "arch_sess_1/req_1");

        // GateKey::new with &str / String via impl Into
        let key2 = GateKey::new("arch_sess_2", "req_2");
        assert_eq!(key2.to_string_key(), "arch_sess_2/req_2");
    }
}
