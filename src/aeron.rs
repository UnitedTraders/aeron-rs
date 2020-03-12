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

use crate::context::Context;
use crate::utils::memory_mapped_file::MemoryMappedFile;
use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::concurrent::ring_buffer::ManyToOneRingBuffer;
use crate::driver_proxy::DriverProxy;
use crate::concurrent::broadcast::broadcast_receiver::BroadcastReceiver;
use crate::concurrent::broadcast::copy_broadcast_receiver::CopyBroadcastReceiver;
use crate::concurrent::strategies::SleepingIdleStrategy;

/**
 * Aeron entry point for communicating to the Media Driver for creating {@link Publication}s and {@link Subscription}s.
 * Use a {@link Context} to configure the Aeron object.
 * <p>
 * A client application requires only one Aeron object per Media Driver.
 */
struct Aeron<'a> {
    random_device: rnd_device,
    random_engine: rnd_engine,
    session_id_distribution: uniform_int_distribution,

    context: Context,

    cnc_buffer: MemoryMappedFile, // May be it should be Arc ???

    to_driver_atomic_buffer: AtomicBuffer,
    to_clients_atomic_buffer: AtomicBuffer,
    counters_metadata_buffer: AtomicBuffer,
    counters_value_buffer: AtomicBuffer,

    to_driver_ring_buffer: ManyToOneRingBuffer,
    driver_proxy: DriverProxy<'a>,

    to_clients_broadcast_receiver: BroadcastReceiver,
    to_clients_copy_receiver: CopyBroadcastReceiver,

    conductor: ClientConductor,
    m_idleStrategy: SleepingIdleStrategy,
    AgentRunner<ClientConductor, SleepingIdleStrategy> self.conductorRunner;
    AgentInvoker<ClientConductor> self.conductorInvoker;

    MemoryMappedFile::ptr_t mapCncFile(Context& context);
}

class Aeron
{
public:
/**
 * Create an Aeron instance and connect to the media driver.
 * <p>
 * Threads required for interacting with the media driver are created and managed within the Aeron instance.
 *
 * @param context for configuration of the client.
 */
Aeron(Context& context);

~Aeron();

/**
 * Indicate if the instance is closed and can not longer be used.
 *
 * @return true is the instance is closed and can no longer be used, otherwise false.
 */
inline bool isClosed()
{
return self.conductor.isClosed();
}

/**
 * Create an Aeron instance and connect to the media driver.
 * <p>
 * Threads required for interacting with the media driver are created and managed within the Aeron instance.
 *
 * @param context for configuration of the client.
 * @return the new Aeron instance connected to the Media Driver.
 */
inline static std::shared_ptr<Aeron> connect(Context& context)
{
return std::make_shared<Aeron>(context);
}

/**
 * Create an Aeron instance and connect to the media driver.
 * <p>
 * Threads required for interacting with the media driver are created and managed within the Aeron instance.
 *
 * @return the new Aeron instance connected to the Media Driver.
 */
inline static std::shared_ptr<Aeron> connect()
{
Context ctx;

return std::make_shared<Aeron>(ctx);
}

/**
 * Add a {@link Publication} for publishing messages to subscribers
 *
 * This function returns immediately and does not wait for the response from the media driver. The returned
 * registration id is to be used to determine the status of the command with the media driver.
 *
 * @param channel for sending the messages known to the media layer.
 * @param streamId within the channel scope.
 * @return registration id for the publication
 */
inline std::int64_t addPublication(const std::string& channel, std::int32_t streamId)
{
return self.conductor.addPublication(channel, streamId);
}

/**
 * Retrieve the Publication associated with the given registrationId.
 *
 * This method is non-blocking.
 *
 * The value returned is dependent on what has occurred with respect to the media driver:
 *
 * - If the registrationId is unknown, then a nullptr is returned.
 * - If the media driver has not answered the add command, then a nullptr is returned.
 * - If the media driver has successfully added the Publication then what is returned is the Publication.
 * - If the media driver has returned an error, this method will throw the error returned.
 *
 * @see Aeron::addPublication
 *
 * @param registrationId of the Publication returned by Aeron::addPublication
 * @return Publication associated with the registrationId
 */
inline std::shared_ptr<Publication> findPublication(std::int64_t registrationId)
{
return self.conductor.findPublication(registrationId);
}

/**
 * Add an {@link ExclusivePublication} for publishing messages to subscribers from a single thread.
 *
 * @param channel  for sending the messages known to the media layer.
 * @param streamId within the channel scope.
 * @return registration id for the publication
 */
inline std::int64_t addExclusivePublication(const std::string& channel, std::int32_t streamId)
{
return self.conductor.addExclusivePublication(channel, streamId);
}

/**
 * Retrieve the ExclusivePublication associated with the given registrationId.
 *
 * This method is non-blocking.
 *
 * The value returned is dependent on what has occurred with respect to the media driver:
 *
 * - If the registrationId is unknown, then a nullptr is returned.
 * - If the media driver has not answered the add command, then a nullptr is returned.
 * - If the media driver has successfully added the ExclusivePublication then what is returned is the ExclusivePublication.
 * - If the media driver has returned an error, this method will throw the error returned.
 *
 * @see Aeron::addExclusivePublication
 *
 * @param registrationId of the ExclusivePublication returned by Aeron::addExclusivePublication
 * @return ExclusivePublication associated with the registrationId
 */
inline std::shared_ptr<ExclusivePublication> findExclusivePublication(std::int64_t registrationId)
{
return self.conductor.findExclusivePublication(registrationId);
}

/**
 * Add a new {@link Subscription} for subscribing to messages from publishers.
 *
 * This function returns immediately and does not wait for the response from the media driver. The returned
 * registration id is to be used to determine the status of the command with the media driver.
 *
 * @param channel  for receiving the messages known to the media layer.
 * @param streamId within the channel scope.
 * @return registration id for the subscription
 */
inline std::int64_t addSubscription(const std::string& channel, std::int32_t streamId)
{
return self.conductor.addSubscription(
channel, streamId, m_context.m_onAvailableImageHandler, m_context.m_onUnavailableImageHandler);
}

/**
 * Add a new {@link Subscription} for subscribing to messages from publishers.
 *
 * This method will override the default handlers from the {@link Context}.
 *
 * @param channel                 for receiving the messages known to the media layer.
 * @param streamId                within the channel scope.
 * @param availableImageHandler   called when {@link Image}s become available for consumption.
 * @param unavailableImageHandler called when {@link Image}s go unavailable for consumption.
 * @return registration id for the subscription
 */
inline std::int64_t addSubscription(
const std::string& channel,
std::int32_t streamId,
const on_available_image_t &onAvailableImageHandler,
const on_unavailable_image_t &onUnavailableImageHandler)
{
return self.conductor.addSubscription(channel, streamId, onAvailableImageHandler, onUnavailableImageHandler);
}

/**
 * Retrieve the Subscription associated with the given registrationId.
 *
 * This method is non-blocking.
 *
 * The value returned is dependent on what has occurred with respect to the media driver:
 *
 * - If the registrationId is unknown, then a nullptr is returned.
 * - If the media driver has not answered the add command, then a nullptr is returned.
 * - If the media driver has successfully added the Subscription then what is returned is the Subscription.
 * - If the media driver has returned an error, this method will throw the error returned.
 *
 * @see Aeron::addSubscription
 *
 * @param registrationId of the Subscription returned by Aeron::addSubscription
 * @return Subscription associated with the registrationId
 */
inline std::shared_ptr<Subscription> findSubscription(std::int64_t registrationId)
{
return self.conductor.findSubscription(registrationId);
}

/**
 * Generate the next correlation id that is unique for the connected Media Driver.
 *
 * This is useful generating correlation identifiers for pairing requests with responses in a clients own
 * application protocol.
 *
 * This method is thread safe and will work across processes that all use the same media driver.
 *
 * @return next correlation id that is unique for the Media Driver.
 */
inline std::int64_t nextCorrelationId()
{
self.conductor.ensureOpen();
return self.to_driver_ring_buffer.nextCorrelationId();
}

/**
 * Allocate a counter on the media driver and return a {@link Counter} for it.
 *
 * @param typeId      for the counter.
 * @param keyBuffer   containing the optional key for the counter.
 * @param keyLength   of the key in the keyBuffer.
 * @param label       for the counter.
 * @return registration id for the Counter
 */
inline std::int64_t addCounter(
std::int32_t typeId,
const std::uint8_t *keyBuffer,
std::size_t keyLength,
const std::string& label)
{
return self.conductor.addCounter(typeId, keyBuffer, keyLength, label);
}

/**
 * Retrieve the Counter associated with the given registrationId.
 *
 * This method is non-blocking.
 *
 * The value returned is dependent on what has occurred with respect to the media driver:
 *
 * - If the registrationId is unknown, then a nullptr is returned.
 * - If the media driver has not answered the add command, then a nullptr is returned.
 * - If the media driver has successfully added the Counter then what is returned is the Counter.
 * - If the media driver has returned an error, this method will throw the error returned.
 *
 * @see Aeron::addCounter
 *
 * @param registrationId of the Counter returned by Aeron::addCounter
 * @return Counter associated with the registrationId
 */
inline std::shared_ptr<Counter> findCounter(std::int64_t registrationId)
{
return self.conductor.findCounter(registrationId);
}

/**
 * Add a handler to the list to be called when a counter becomes available.
 *
 * @param handler to be added to the available counters list.
 */
inline void addAvailableCounterHandler(const on_available_counter_t& handler)
{
self.conductor.addAvailableCounterHandler(handler);
}

/**
 * Remove a handler from the list to be called when a counter becomes available.
 *
 * @param handler to be removed from the available counters list.
 */
inline void removeAvailableCounterHandler(const on_available_counter_t& handler)
{
self.conductor.removeAvailableCounterHandler(handler);
}

/**
 * Add a handler to the list to be called when a counter becomes unavailable.
 *
 * @param handler to be added to the unavailable counters list.
 */
inline void addUnavailableCounterHandler(const on_unavailable_counter_t& handler)
{
self.conductor.addUnavailableCounterHandler(handler);
}

/**
 * Remove a handler from the list to be called when a counter becomes unavailable.
 *
 * @param handler to be removed from the unavailable counters list.
 */
inline void removeUnavailableCounterHandler(const on_unavailable_counter_t& handler)
{
self.conductor.removeUnavailableCounterHandler(handler);
}

/**
 * Add a handler to the list to be called when the client is closed.
 *
 * @param handler to be added to the close client handlers list.
 */
inline void addCloseClientHandler(const on_close_client_t & handler)
{
self.conductor.addCloseClientHandler(handler);
}

/**
 * Remove a handler from the list to be called when the client is closed.
 *
 * @param handler to be removed from the close client handlers list.
 */
inline void removeCloseClientHandler(const on_close_client_t & handler)
{
self.conductor.removeCloseClientHandler(handler);
}

/**
 * Return the AgentInvoker for the client conductor.
 *
 * @return AgentInvoker for the conductor.
 */
inline AgentInvoker<ClientConductor>& conductorAgentInvoker()
{
return self.conductorInvoker;
}

/**
 * Return whether the AgentInvoker is used or not.
 *
 * @return true if AgentInvoker used or false if not.
 */
inline bool usesAgentInvoker() const
{
return m_context.m_useConductorAgentInvoker;
}

/**
 * Get the CountersReader for the Aeron media driver counters.
 *
 * @return CountersReader for the Aeron media driver in use.
 */
inline CountersReader& countersReader()
{
return self.conductor.countersReader();
}

/**
 * Get the client identity that has been allocated for communicating with the media driver.
 *
 * @return the client identity that has been allocated for communicating with the media driver.
 */
inline std::int64_t clientId() const
{
return self.driver_proxy.clientId();
}

/**
 * Get the Aeron Context object used in construction of the Aeron instance.
 *
 * @return Context instance in use.
 */
inline Context& context()
{
return m_context;
}

inline const Context& context() const
{
return m_context;
}

/**
 * Return the static version and build string for the binary library.
 *
 * @return static version and build string for the binary library.
 */
static std::string version();

// CPPPPPPP
static const std::chrono::duration<long, std::milli> IDLE_SLEEP_MS(4);
static const std::chrono::duration<long, std::milli> IDLE_SLEEP_MS_1(1);
static const std::chrono::duration<long, std::milli> IDLE_SLEEP_MS_16(16);
static const std::chrono::duration<long, std::milli> IDLE_SLEEP_MS_100(100);

static const char* AGENT_NAME = "client-conductor";

Aeron::Aeron(Context &context) :
self.random_engine(self.random_device()),
self.session_id_distribution(-INT_MAX, INT_MAX),
m_context(context.conclude()),
self.cnc_buffer(mapCncFile(m_context)),
self.to_driver_atomic_buffer(CncFileDescriptor::createToDriverBuffer(self.cnc_buffer)),
self.to_clients_atomic_buffer(CncFileDescriptor::createToClientsBuffer(self.cnc_buffer)),
self.counters_metadata_buffer(CncFileDescriptor::createCounterMetadataBuffer(self.cnc_buffer)),
self.counters_value_buffer(CncFileDescriptor::createCounterValuesBuffer(self.cnc_buffer)),
self.to_driver_ring_buffer(self.to_driver_atomic_buffer),
self.driver_proxy(self.to_driver_ring_buffer),
self.to_clients_broadcast_receiver(self.to_clients_atomic_buffer),
self.to_clients_copy_receiver(self.to_clients_broadcast_receiver),
self.conductor(
currentTimeMillis,
self.driver_proxy,
self.to_clients_copy_receiver,
self.counters_metadata_buffer,
self.counters_value_buffer,
m_context.m_onNewPublicationHandler,
m_context.m_onNewExclusivePublicationHandler,
m_context.m_onNewSubscriptionHandler,
m_context.m_exceptionHandler,
m_context.m_onAvailableCounterHandler,
m_context.m_onUnavailableCounterHandler,
m_context.m_onCloseClientHandler,
m_context.m_mediaDriverTimeout,
m_context.m_resourceLingerTimeout,
CncFileDescriptor::clientLivenessTimeout(self.cnc_buffer),
m_context.m_preTouchMappedMemory),
m_idleStrategy(IDLE_SLEEP_MS),
self.conductorRunner(self.conductor, m_idleStrategy, m_context.m_exceptionHandler, AGENT_NAME),
self.conductorInvoker(self.conductor, m_context.m_exceptionHandler)
{
if (m_context.m_useConductorAgentInvoker)
{
self.conductorInvoker.start();
}
else
{
self.conductorRunner.start();
}
}

Aeron::~Aeron()
{
if (m_context.m_useConductorAgentInvoker)
{
self.conductorInvoker.close();
}
else
{
self.conductorRunner.close();
}

// memory mapped files should be free'd by the destructor of the shared_ptr
}

inline MemoryMappedFile::ptr_t Aeron::mapCncFile(Context &context)
{
const long long startMs = currentTimeMillis();
MemoryMappedFile::ptr_t cncBuffer;

while (true)
{
while (MemoryMappedFile::getFileSize(context.cncFileName().c_str()) <= 0)
{
if (currentTimeMillis() > (startMs + context.m_mediaDriverTimeout))
{
throw DriverTimeoutException("CnC file not created: " + context.cncFileName(), SOURCEINFO);
}

std::this_thread::sleep_for(IDLE_SLEEP_MS_16);
}

cncBuffer = MemoryMappedFile::mapExisting(context.cncFileName().c_str());

std::int32_t cncVersion = 0;

while (0 == (cncVersion = CncFileDescriptor::cncVersionVolatile(cncBuffer)))
{
if (currentTimeMillis() > (startMs + context.m_mediaDriverTimeout))
{
throw DriverTimeoutException(
"CnC file is created but not initialised: " + context.cncFileName(), SOURCEINFO);
}

std::this_thread::sleep_for(IDLE_SLEEP_MS_1);
}

if (semanticVersionMajor(cncVersion) != semanticVersionMajor(CncFileDescriptor::CNC_VERSION))
{
throw AeronException("Aeron CnC version does not match:"
" app=" + semanticVersionToString(CncFileDescriptor::CNC_VERSION) +
" file=" + semanticVersionToString(cncVersion),
SOURCEINFO);
}

AtomicBuffer toDriverBuffer(CncFileDescriptor::createToDriverBuffer(cncBuffer));
ManyToOneRingBuffer ringBuffer(toDriverBuffer);

while (0 == ringBuffer.consumerHeartbeatTime())
{
if (currentTimeMillis() > (startMs + context.m_mediaDriverTimeout))
{
throw DriverTimeoutException(std::string("no driver heartbeat detected"), SOURCEINFO);
}

std::this_thread::sleep_for(IDLE_SLEEP_MS_1);
}

const long long timeMs = currentTimeMillis();
if (ringBuffer.consumerHeartbeatTime() < (timeMs - context.m_mediaDriverTimeout))
{
if (timeMs > (startMs + context.m_mediaDriverTimeout))
{
throw DriverTimeoutException(std::string("no driver heartbeat detected"), SOURCEINFO);
}

cncBuffer = nullptr;

std::this_thread::sleep_for(IDLE_SLEEP_MS_100);
continue;
}

break;
}

return cncBuffer;
}

std::string Aeron::version()
{
return std::string("aeron version " AERON_VERSION_TXT " built " __DATE__ " " __TIME__);
}

}


