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

/**
* List of event types used in the control protocol between the media driver and the core.
*/

/** Add Publication */
pub(crate) const ADD_PUBLICATION: i32 = 0x01;
/** Remove Publication */
pub(crate) const REMOVE_PUBLICATION: i32 = 0x02;
/** Add Exclusive Publication */
pub(crate) const ADD_EXCLUSIVE_PUBLICATION: i32 = 0x03;
/** Add Subscriber */
pub(crate) const ADD_SUBSCRIPTION: i32 = 0x04;
/** Remove Subscriber */
pub(crate) const REMOVE_SUBSCRIPTION: i32 = 0x05;
/** Keepalive from Client */
pub(crate) const CLIENT_KEEPALIVE: i32 = 0x06;
/** Add Destination */
pub(crate) const ADD_DESTINATION: i32 = 0x07;
/** Remove Destination */
pub(crate) const REMOVE_DESTINATION: i32 = 0x08;
/** Add Counter */
pub(crate) const ADD_COUNTER: i32 = 0x09;
/** Remove Counter */
pub(crate) const REMOVE_COUNTER: i32 = 0x0A;
/** Client Close */
pub(crate) const CLIENT_CLOSE: i32 = 0x0B;
/** Add Destination for existing Subscription */
pub(crate) const ADD_RCV_DESTINATION: i32 = 0x0C;
/** Remove Destination for existing Subscription */
pub(crate) const REMOVE_RCV_DESTINATION: i32 = 0x0D;
/** Request driver run termination hook */
pub(crate) const TERMINATE_DRIVER: i32 = 0x0E;

// Media Driver to Clients

/** Error Response */
pub(crate) const ON_ERROR: i32 = 0x0F01;
/** New image Buffer Notification */
pub(crate) const ON_AVAILABLE_IMAGE: i32 = 0x0F02;
/** New pub(crate)lication Buffer Notification */
pub(crate) const ON_PUBLICATION_READY: i32 = 0x0F03;
/** Operation Succeeded */
pub(crate) const ON_OPERATION_SUCCESS: i32 = 0x0F04;
/** Inform client of timeout and removal of inactive image */
pub(crate) const ON_UNAVAILABLE_IMAGE: i32 = 0x0F05;
/** New Exclusive Publication Buffer notification */
pub(crate) const ON_EXCLUSIVE_PUBLICATION_READY: i32 = 0x0F06;
/** New subscription notification */
pub(crate) const ON_SUBSCRIPTION_READY: i32 = 0x0F07;
/** New counter notification */
pub(crate) const ON_COUNTER_READY: i32 = 0x0F08;
/** inform clients of removal of counter */
pub(crate) const ON_UNAVAILABLE_COUNTER: i32 = 0x0F09;
/** inform clients of client timeout */
pub(crate) const ON_CLIENT_TIMEOUT: i32 = 0x0F0A;
