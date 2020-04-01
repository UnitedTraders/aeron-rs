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

pub const DEFAULT_CHANNEL: &str = "aeron:udp?endpoint=localhost:40123";
pub const DEFAULT_PING_CHANNEL: &str = "aeron:udp?endpoint=localhost:40123";
pub const DEFAULT_PONG_CHANNEL: &str = "aeron:udp?endpoint=localhost:40124";
pub const DEFAULT_STREAM_ID: i32 = 1001;
pub const DEFAULT_PING_STREAM_ID: i32 = 1002;
pub const DEFAULT_PONG_STREAM_ID: i32 = 1003;
pub const DEFAULT_NUMBER_OF_WARM_UP_MESSAGES: i64 = 100_000;
pub const DEFAULT_NUMBER_OF_MESSAGES: i64 = 10_000_000;
pub const DEFAULT_MESSAGE_LENGTH: i32 = 32;
pub const DEFAULT_LINGER_TIMEOUT_MS: i32 = 0;
pub const DEFAULT_FRAGMENT_COUNT_LIMIT: i32 = 10;
pub const DEFAULT_RANDOM_MESSAGE_LENGTH: bool = false;
pub const DEFAULT_PUBLICATION_RATE_PROGRESS: bool = false;
