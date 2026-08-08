//! Phase-1 auth hooks (allow-all; token parse retained for later).

use serde::Deserialize;

/// Authentication mode for HTTP/WS entry points.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AuthMode {
    /// Accept all connections; do not require credentials.
    #[default]
    Disabled,
}

/// Optional query parameters parsed from `/ws`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct WsAuthQuery {
    /// Optional bearer-style access token (not enforced in [`AuthMode::Disabled`]).
    pub access_token: Option<String>,
}

impl AuthMode {
    /// Extract an access token from the WS query (and optional Authorization header).
    ///
    /// This is a hook for later auth modes; Phase 1 never rejects based on it.
    pub fn extract_access_token<'a>(
        &self,
        query: &'a WsAuthQuery,
        authorization_header: Option<&'a str>,
    ) -> Option<&'a str> {
        if let Some(token) = query.access_token.as_deref() {
            return Some(token);
        }
        authorization_header
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(str::trim)
            .filter(|t| !t.is_empty())
    }

    /// Authorize a WebSocket upgrade. [`AuthMode::Disabled`] always allows.
    ///
    /// Returns an optional user id when auth is enforced in later phases.
    pub fn authorize_ws(
        &self,
        _token: Option<&str>,
    ) -> Result<Option<String>, AuthError> {
        match self {
            AuthMode::Disabled => Ok(None),
        }
    }
}

/// Auth failures (unused while [`AuthMode::Disabled`] is the only mode).
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("missing access token")]
    MissingToken,
    #[error("invalid access token")]
    InvalidToken,
}

#[cfg(test)]
mod tests {
    use super::{AuthMode, WsAuthQuery};

    #[test]
    fn disabled_allows_without_token() {
        let mode = AuthMode::Disabled;
        assert!(mode.authorize_ws(None).unwrap().is_none());
    }

    #[test]
    fn extracts_query_token_hook() {
        let mode = AuthMode::Disabled;
        let query = WsAuthQuery {
            access_token: Some("abc".into()),
        };
        assert_eq!(mode.extract_access_token(&query, None), Some("abc"));
    }
}
