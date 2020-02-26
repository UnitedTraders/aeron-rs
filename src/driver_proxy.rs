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


use crate::concurrent::ring_buffer::ManyToOneRingBuffer;
use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::utils::types::Index;

pub(crate) struct DriverProxy<'a> {
    to_driver_command_buffer: &'a ManyToOneRingBuffer,
    client_id: i64,
}

impl<'a> DriverProxy<'a> {
    pub fn new(to_driver_command_buffer: &'a ManyToOneRingBuffer) -> Self {
        Self {
            to_driver_command_buffer,
            client_id: to_driver_command_buffer.next_correlation_id()
        }
    }


    pub fn time_of_last_driver_keepalive(&self) -> i64 {
        self.to_driver_command_buffer.consumer_heartbeat_time()
    }

    pub fn client_id(&self) -> i64 {
        self.client_id
    }

    pub fn add_publication(&self, channel: &str, stream_id: i32) -> i64 {
        let correlation_id = self.to_driver_command_buffer.next_correlation_id();

        writeCommandToDriver([&](AtomicBuffer & buffer, util::index_t & length) {
            PublicationMessageFlyweight publicationMessage(buffer, 0);

            publicationMessage.clientId(self.client_id);
            publicationMessage.correlation_id(correlation_id);
            publicationMessage.stream_id(stream_id);
            publicationMessage.channel(channel);

            length = publicationMessage.length();

            return ControlProtocolEvents::ADD_PUBLICATION;
        });

        correlation_idx`
    }

    std::int64_t addExclusivePublication(const std::string & channel, std::int32_t stream_id)
    {
    std::int64_t correlation_id = self.to_driver_command_buffer.next_correlation_id();

    writeCommandToDriver([ & ](AtomicBuffer & buffer, util::index_t & length)
    {
    PublicationMessageFlyweight publicationMessage(buffer, 0);

    publicationMessage.clientId( self.client_id);
    publicationMessage.correlation_id(correlation_id);
    publicationMessage.stream_id(stream_id);
    publicationMessage.channel(channel);

    length = publicationMessage.length();

    return ControlProtocolEvents::ADD_EXCLUSIVE_PUBLICATION;
    });

    return correlation_id;
    }

    std::int64_t removePublication(std::int64_t registrationId)
    {
    std::int64_t correlation_id = self.to_driver_command_buffer.next_correlation_id();

    writeCommandToDriver([ & ](AtomicBuffer & buffer, util::index_t & length)
    {
    RemoveMessageFlyweight removeMessage(buffer, 0);

    removeMessage.clientId( self.client_id);
    removeMessage.correlation_id(correlation_id);
    removeMessage.registrationId(registrationId);

    length = RemoveMessageFlyweight::length();

    return ControlProtocolEvents::REMOVE_PUBLICATION;
    });

    return correlation_id;
    }

    std::int64_t addSubscription(const std::string & channel, std::int32_t stream_id)
    {
    std::int64_t correlation_id = self.to_driver_command_buffer.next_correlation_id();

    writeCommandToDriver([ & ](AtomicBuffer & buffer, util::index_t & length)
    {
    SubscriptionMessageFlyweight subscriptionMessage(buffer, 0);

    subscriptionMessage.clientId( self.client_id);
    subscriptionMessage.registrationCorrelationId( - 1);
    subscriptionMessage.correlation_id(correlation_id);
    subscriptionMessage.stream_id(stream_id);
    subscriptionMessage.channel(channel);

    length = subscriptionMessage.length();

    return ControlProtocolEvents::ADD_SUBSCRIPTION;
    });

    return correlation_id;
    }

    std::int64_t removeSubscription(std::int64_t registrationId)
    {
    std::int64_t correlation_id = self.to_driver_command_buffer.next_correlation_id();

    writeCommandToDriver([ & ](AtomicBuffer & buffer, util::index_t & length)
    {
    RemoveMessageFlyweight removeMessage(buffer, 0);

    removeMessage.clientId( self.client_id);
    removeMessage.correlation_id(correlation_id);
    removeMessage.registrationId(registrationId);

    length = RemoveMessageFlyweight::length();

    return ControlProtocolEvents::REMOVE_SUBSCRIPTION;
    });
    return correlation_id;
    }

    void sendClientKeepalive()
    {
    writeCommandToDriver([ & ](AtomicBuffer & buffer, util::index_t & length)
    {
    CorrelatedMessageFlyweight correlatedMessage(buffer, 0);

    correlatedMessage.clientId( self.client_id);
    correlatedMessage.correlation_id(0);

    length = CORRELATED_MESSAGE_LENGTH;

    return ControlProtocolEvents::CLIENT_KEEPALIVE;
    });
    }

    std::int64_t addDestination(std::int64_t publicationRegistrationId, const std::string & channel)
    {
    std::int64_t correlation_id = self.to_driver_command_buffer.next_correlation_id();

    writeCommandToDriver([ & ](AtomicBuffer & buffer, util::index_t & length)
    {
    DestinationMessageFlyweight addMessage(buffer, 0);

    addMessage.clientId( self.client_id);
    addMessage.registrationId(publicationRegistrationId);
    addMessage.correlation_id(correlation_id);
    addMessage.channel(channel);

    length = addMessage.length();

    return ControlProtocolEvents::ADD_DESTINATION;
    });

    return correlation_id;
    }

    std::int64_t removeDestination(std::int64_t publicationRegistrationId, const std::string & channel)
    {
    std::int64_t correlation_id = self.to_driver_command_buffer.next_correlation_id();

    writeCommandToDriver([ & ](AtomicBuffer & buffer, util::index_t & length)
    {
    DestinationMessageFlyweight removeMessage(buffer, 0);

    removeMessage.clientId( self.client_id);
    removeMessage.registrationId(publicationRegistrationId);
    removeMessage.correlation_id(correlation_id);
    removeMessage.channel(channel);

    length = removeMessage.length();

    return ControlProtocolEvents::REMOVE_DESTINATION;
    });

    return correlation_id;
    }

    std::int64_t addRcvDestination(std::int64_t subscriptionRegistrationId, const std::string & channel)
    {
    std::int64_t correlation_id = self.to_driver_command_buffer.next_correlation_id();

    writeCommandToDriver([ & ](AtomicBuffer & buffer, util::index_t & length)
    {
    DestinationMessageFlyweight addMessage(buffer, 0);

    addMessage.clientId( self.client_id);
    addMessage.registrationId(subscriptionRegistrationId);
    addMessage.correlation_id(correlation_id);
    addMessage.channel(channel);

    length = addMessage.length();

    return ControlProtocolEvents::ADD_RCV_DESTINATION;
    });

    return correlation_id;
    }

    std::int64_t removeRcvDestination(std::int64_t subscriptionRegistrationId, const std::string & channel)
    {
    std::int64_t correlation_id = self.to_driver_command_buffer.next_correlation_id();

    writeCommandToDriver([ & ](AtomicBuffer & buffer, util::index_t & length)
    {
    DestinationMessageFlyweight removeMessage(buffer, 0);

    removeMessage.clientId( self.client_id);
    removeMessage.registrationId(subscriptionRegistrationId);
    removeMessage.correlation_id(correlation_id);
    removeMessage.channel(channel);

    length = removeMessage.length();

    return ControlProtocolEvents::REMOVE_RCV_DESTINATION;
    });

    return correlation_id;
    }

    std::int64_t addCounter(std::int32_t typeId, const std::uint8_t * key, std::size_t keyLength, const std::string & label)
    {
    std::int64_t correlation_id = self.to_driver_command_buffer.next_correlation_id();

    writeCommandToDriver([ & ](AtomicBuffer & buffer, util::index_t & length)
    {
    CounterMessageFlyweight command(buffer, 0);

    command.clientId( self.client_id);
    command.correlation_id(correlation_id);
    command.typeId(typeId);
    command.keyBuffer(key, keyLength);
    command.label(label);

    length = command.length();

    return ControlProtocolEvents::ADD_COUNTER;
    });

    return correlation_id;
    }

    std::int64_t removeCounter(std::int64_t registrationId)
    {
    std::int64_t correlation_id = self.to_driver_command_buffer.next_correlation_id();

    writeCommandToDriver([ & ](AtomicBuffer & buffer, util::index_t & length)
    {
    RemoveMessageFlyweight command(buffer, 0);

    command.clientId( self.client_id);
    command.correlation_id(correlation_id);
    command.registrationId(registrationId);

    length = RemoveMessageFlyweight::length();

    return ControlProtocolEvents::REMOVE_COUNTER;
    });

    return correlation_id;
    }

    std::int64_t clientClose()
    {
    std::int64_t correlation_id = self.to_driver_command_buffer.next_correlation_id();

    writeCommandToDriver([ & ](AtomicBuffer& buffer, util::index_t & length)
    {
    CorrelatedMessageFlyweight correlatedMessage(buffer, 0);

    correlatedMessage.clientId( self.client_id);
    correlatedMessage.correlation_id(correlation_id);

    length = CORRELATED_MESSAGE_LENGTH;

    return ControlProtocolEvents::CLIENT_CLOSE;
    });

    return correlation_id;
    }

    void terminateDriver(const std::uint8_t * tokenBuffer, std::size_t tokenLength)
    {
    writeCommandToDriver([ & ](AtomicBuffer & buffer, util::index_t & length)
    {
    TerminateDriverFlyweight request(buffer, 0);

    request.clientId( self.client_id);
    request.correlation_id( - 1);
    request.tokenBuffer(tokenBuffer, tokenLength);

    length = request.length();

    return ControlProtocolEvents::TERMINATE_DRIVER;
    });
    }

    private:
    typedef std::array < std::uint8_t, 512 > driver_proxy_command_buffer_t;


    fn write_command_to_driver<T>(&self, filler: T) {
        let message_buffer = DriverProxyCommandBuffer::default();
        let buffer = AtomicBuffer::new(&message_buffer.data[0] as *mut u8, message_buffer.data.len() as Index);
        let length = buffer.capacity();

        let msg_type_id = filler(buffer, length);

        if !self.to_driver_command_buffer.write(msg_type_id, buffer, 0, length) {
            throw util::IllegalStateException("couldn't write command to driver", SOURCEINFO);
        }
    }
}

// 16 byte alignment was used in C++ code. To make pointers work it is enough to
// align this buffer at 8 bytes (on 64 bit architecture). And to make access to
// the buffer (may be) faster it could be aligned to CACHE_LINE_LENGTH (64 bytes for modern x86 CPUs)
#[repr(C, align(16))]
struct DriverProxyCommandBuffer {
    data: [u8; 512]
}

impl Default for DriverProxyCommandBuffer {
    fn default() -> Self {
        Self {
            data: [0;512] // zero the memory
        }
    }
}

