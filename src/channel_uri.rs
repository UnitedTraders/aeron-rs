/*
 * Copyright 2020 UT OVERSEAS INC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::{
    collections::HashMap,
    fmt::{self, Display, Formatter},
    sync::{Arc, Mutex},
};

use crate::utils::errors::{AeronError, IllegalArgumentError, IllegalStateError};

pub const SPY_QUALIFIER: &str = "aeron-spy";
pub const AERON_SCHEME: &str = "aeron";

pub const AERON_PREFIX: &str = "aeron:";
pub const IPC_MEDIA: &str = "ipc";
pub const UDP_MEDIA: &str = "udp";
pub const IPC_CHANNEL: &str = "aeron:ipc";
pub const SPY_PREFIX: &str = "aeron-spy:";
pub const ENDPOINT_PARAM_NAME: &str = "endpoint";
pub const INTERFACE_PARAM_NAME: &str = "interface";
pub const INITIAL_TERM_ID_PARAM_NAME: &str = "init-term-id";
pub const TERM_ID_PARAM_NAME: &str = "term-id";
pub const TERM_OFFSET_PARAM_NAME: &str = "term-offset";
pub const TERM_LENGTH_PARAM_NAME: &str = "term-length";
pub const MTU_LENGTH_PARAM_NAME: &str = "mtu";
pub const TTL_PARAM_NAME: &str = "ttl";
pub const MDC_CONTROL_PARAM_NAME: &str = "control";
pub const MDC_CONTROL_MODE_PARAM_NAME: &str = "control-mode";
pub const MDC_CONTROL_MODE_MANUAL: &str = "manual";
pub const MDC_CONTROL_MODE_DYNAMIC: &str = "dynamic";
pub const SESSION_ID_PARAM_NAME: &str = "session-id";
pub const LINGER_PARAM_NAME: &str = "linger";
pub const RELIABLE_STREAM_PARAM_NAME: &str = "reliable";
pub const TAGS_PARAM_NAME: &str = "tags";
pub const TAG_PREFIX: &str = "tag:";
pub const SPARSE_PARAM_NAME: &str = "sparse";
pub const ALIAS_PARAM_NAME: &str = "alias";
pub const EOS_PARAM_NAME: &str = "eos";
pub const TETHER_PARAM_NAME: &str = "tether";
pub const GROUP_PARAM_NAME: &str = "group";
pub const REJOIN_PARAM_NAME: &str = "rejoin";

pub const CONGESTION_CONTROL_PARAM_NAME: &str = "cc";

#[derive(Debug)]
pub enum State {
    Media,
    ParamsKey,
    ParamsValue,
}

#[derive(Debug)]
pub struct ChannelUri {
    prefix: String,
    media: String,
    params: HashMap<String, String>,
}

impl ChannelUri {
    pub fn new(prefix: String, media: String, params: HashMap<String, String>) -> Self {
        Self { prefix, media, params }
    }

    #[inline]
    pub fn prefix(&self) -> String {
        self.prefix.clone()
    }

    #[inline]
    pub fn set_prefix(&mut self, value: String) {
        self.prefix = value;
    }

    #[inline]
    pub fn media(&self) -> String {
        self.media.clone()
    }

    #[inline]
    pub fn set_media(&mut self, value: String) {
        self.media = value;
    }

    #[inline]
    pub const fn scheme() -> &'static str {
        AERON_SCHEME
    }

    #[inline]
    pub fn get(&self, key: &str) -> &str {
        if let Some(value) = self.params.get(key) {
            value
        } else {
            ""
        }
    }

    #[inline]
    pub fn get_or_default<'a>(&'a self, key: &str, default_value: &'a str) -> &'a str {
        if let Some(value) = self.params.get(key) {
            value
        } else {
            default_value
        }
    }

    #[inline]
    pub fn put(&mut self, key: &str, value: String) {
        self.params.insert(String::from(key), value);
    }

    #[inline]
    pub fn remove(&mut self, key: &str) -> String {
        if let Some(result) = self.params.remove(key) {
            result
        } else {
            String::default()
        }
    }

    #[inline]
    pub fn contains_key(&self, key: &str) -> bool {
        self.params.get(key).is_some()
    }

    pub fn parse(uri: &str) -> Result<Arc<Mutex<Self>>, AeronError> {
        let mut position = 0;
        let prefix;

        if uri.starts_with(SPY_PREFIX) {
            prefix = SPY_QUALIFIER;
            position = SPY_PREFIX.len();
        } else {
            prefix = "";
        }

        if !&uri[position..].starts_with(AERON_PREFIX) {
            return Err(IllegalArgumentError::UriMustStartWithAeron { uri: uri.to_string() }.into());
        } else {
            position += AERON_PREFIX.len();
        }

        let mut builder = String::new();
        let mut params: HashMap<String, String> = HashMap::new();
        let mut media = String::new();
        let mut key = String::new();
        let mut state = State::Media;

        for i in position..uri.len() {
            let c: char = uri.chars().nth(i).unwrap();
            match state {
                State::Media => match c {
                    '?' => {
                        media = builder.clone();
                        builder.clear();
                        state = State::ParamsKey;
                    }
                    '=' | '|' | ':' => {
                        return Err(IllegalStateError::EncounteredCharacterWithinMediaDefinition {
                            c,
                            index: i,
                            uri: uri.to_string(),
                        }
                        .into());
                    }
                    _ => builder.push(c),
                },
                State::ParamsKey => {
                    if c == '=' {
                        if builder.is_empty() {
                            return Err(IllegalStateError::EmptyKeyNotAllowed {
                                index: i,
                                uri: uri.to_string(),
                            }
                            .into());
                        }
                        key = builder.clone();
                        builder.clear();
                        state = State::ParamsValue;
                    } else {
                        if c == '|' {
                            return Err(IllegalStateError::InvalidEndOfKey {
                                index: i,
                                uri: uri.to_string(),
                            }
                            .into());
                        }
                        builder.push(c);
                    }
                }
                State::ParamsValue => {
                    if c == '|' {
                        params.insert(key.clone(), builder.clone());
                        builder.clear();
                        state = State::ParamsKey;
                    } else {
                        builder.push(c);
                    }
                }
            }
        }

        match state {
            State::Media => {
                media = builder;
                if media != IPC_MEDIA && media != UDP_MEDIA {
                    return Err(IllegalArgumentError::UnknownMedia(media).into());
                }
            }
            State::ParamsValue => {
                params.insert(key, builder);
            }
            _ => {
                return Err(IllegalArgumentError::NoMoreInputFound { state }.into());
            }
        }

        Ok(Arc::new(Mutex::new(ChannelUri::new(String::from(prefix), media, params))))
    }

    #[inline]
    pub fn add_session_id(channel: &str, session_id: i32) -> Result<String, AeronError> {
        let channel_uri = Self::parse(channel)?;
        let mut channel_guard = channel_uri.lock().unwrap();
        channel_guard.put(SESSION_ID_PARAM_NAME, session_id.to_string());

        Ok(channel_guard.to_string())
    }
}

impl Display for ChannelUri {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut sb = String::new();
        if self.prefix.is_empty() {
            sb.reserve((self.params.len() * 20) + 10);
        } else {
            sb.reserve((self.params.len() * 20) + 20);
            sb += &self.prefix;
            if !self.prefix.ends_with(':') {
                sb += ":";
            }
        }

        sb += AERON_PREFIX;
        sb += &self.media;

        if !self.params.is_empty() {
            sb += "?";
            for (key, value) in &self.params {
                sb += &format!("{}={}|", key, value);
            }
            sb.pop();
        }
        write!(f, "{}", &sb)
    }
}

#[cfg(test)]
mod tests {
    use galvanic_assert::matchers::any_value;
    use galvanic_assert::{assert_that, has_structure, structure};

    use crate::channel_uri::{ChannelUri, SPY_QUALIFIER, UDP_MEDIA};
    use crate::channel_uri_string_builder::ChannelUriStringBuilder;
    use crate::utils::errors::AeronError;

    #[test]
    fn should_parse_simple_default_uris() {
        let channel_uri = ChannelUri::parse("aeron:udp").expect("Can't parse uri");
        let channel_guard = channel_uri.lock().unwrap();
        assert_eq!(channel_guard.prefix(), "");
        assert_eq!(channel_guard.media(), "udp");

        let channel_uri = ChannelUri::parse("aeron:ipc").expect("Can't parse uri");
        let channel_guard = channel_uri.lock().unwrap();
        assert_eq!(channel_guard.prefix(), "");
        assert_eq!(channel_guard.media(), "ipc");

        let channel_uri = ChannelUri::parse("aeron-spy:aeron:udp").expect("Can't parse uri");
        let channel_guard = channel_uri.lock().unwrap();
        assert_eq!(channel_guard.prefix(), "aeron-spy");
        assert_eq!(channel_guard.media(), "udp");
    }

    #[test]
    fn should_reject_uri_without_aeron_prefix() {
        let result = ChannelUri::parse(":udp");
        assert_that!(&result.unwrap_err(), has_structure!(AeronError::IllegalArgument[any_value()]));

        let result = ChannelUri::parse("aeron");
        assert_that!(&result.unwrap_err(), has_structure!(AeronError::IllegalArgument[any_value()]));

        let result = ChannelUri::parse("aron:");
        assert_that!(&result.unwrap_err(), has_structure!(AeronError::IllegalArgument[any_value()]));

        let result = ChannelUri::parse("eeron:");
        assert_that!(&result.unwrap_err(), has_structure!(AeronError::IllegalArgument[any_value()]));
    }

    #[test]
    fn should_reject_with_out_of_place_colon() {
        let result = ChannelUri::parse("aeron:udp:");
        assert_that!(&result.unwrap_err(), has_structure!(AeronError::IllegalState[any_value()]));
    }

    #[test]
    fn should_reject_invalid_media() {
        let result = ChannelUri::parse("aeron:ipcsdfgfdhfgf");
        assert_that!(&result.unwrap_err(), has_structure!(AeronError::IllegalArgument[any_value()]));
    }

    #[test]
    fn should_reject_with_missing_query_separator_when_followed_with_params() {
        let result = ChannelUri::parse("aeron:ipc|sparse=true");
        assert_that!(&result.unwrap_err(), has_structure!(AeronError::IllegalState[any_value()]));
    }

    #[test]
    fn should_reject_with_invalid_params() {
        let result = ChannelUri::parse("aeron:udp?endpoint=localhost:4652|-~@{]|=??#s!£$%====");
        assert_that!(&result.unwrap_err(), has_structure!(AeronError::IllegalState[any_value()]));
    }

    #[test]
    fn should_parse_with_single_parameter() {
        let channel_uri = ChannelUri::parse("aeron:udp?endpoint=224.10.9.8").expect("Can't parse uri");
        let channel_guard = channel_uri.lock().unwrap();
        assert_eq!(channel_guard.get("endpoint"), "224.10.9.8");

        let channel_uri = ChannelUri::parse("aeron:udp?address=224.10.9.8").expect("Can't parse uri");
        let channel_guard = channel_uri.lock().unwrap();
        assert_eq!(channel_guard.get("address"), "224.10.9.8");

        let channel_uri = ChannelUri::parse("aeron:udp?endpoint=224.10.9.8").expect("Can't parse uri");
        let channel_guard = channel_uri.lock().unwrap();
        assert_eq!(channel_guard.get("endpoint"), "224.10.9.8");
    }

    #[test]
    fn should_parse_with_multiple_arguments() {
        let channel_uri =
            ChannelUri::parse("aeron:udp?endpoint=224.10.9.8|port=4567|interface=192.168.0.3|ttl=16").expect("Can't parse uri");
        let channel_guard = channel_uri.lock().unwrap();
        assert_eq!(channel_guard.get("endpoint"), "224.10.9.8");
        assert_eq!(channel_guard.get("port"), "4567");
        assert_eq!(channel_guard.get("interface"), "192.168.0.3");
        assert_eq!(channel_guard.get("ttl"), "16");
    }

    #[test]
    fn should_allow_return_default_if_param_not_specified() {
        let channel_uri = ChannelUri::parse("aeron:udp?endpoint=224.10.9.8").expect("Can't parse uri");
        let channel_guard = channel_uri.lock().unwrap();
        assert_eq!(channel_guard.get("endpoint"), "224.10.9.8");
        assert_eq!(channel_guard.get("interface"), "");
        assert_eq!(channel_guard.get_or_default("interface", "192.168.0.0"), "192.168.0.0");
    }

    #[test]
    fn should_round_trip_to_string() {
        let uri_string = String::from("aeron:udp?endpoint=224.10.9.8:777");
        let channel_uri = ChannelUri::parse(&uri_string).expect("Can't parse uri");
        let channel_guard = channel_uri.lock().unwrap();

        assert_eq!(channel_guard.to_string(), uri_string);
    }

    #[test]
    fn should_round_trip_to_string_builder() {
        let mut builder = ChannelUriStringBuilder::default();

        builder.media(UDP_MEDIA).unwrap().endpoint("224.10.9.8:777");
        let uri_string = builder.build();

        let channel_uri = ChannelUri::parse(&uri_string).expect("Can't parse uri");
        let channel_guard = channel_uri.lock().unwrap();
        assert_eq!(channel_guard.to_string(), uri_string);
    }

    #[test]
    fn should_round_trip_to_string_builder_with_prefix() {
        let mut builder = ChannelUriStringBuilder::default();

        builder
            .prefix(SPY_QUALIFIER)
            .unwrap()
            .media(UDP_MEDIA)
            .unwrap()
            .endpoint("224.10.9.8:777");
        let uri_string = builder.build();

        let channel_uri = ChannelUri::parse(&uri_string).expect("Can't parse uri");
        let channel_guard = channel_uri.lock().unwrap();
        assert_eq!(channel_guard.to_string(), uri_string);
    }
}
