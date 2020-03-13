use crate::concurrent::logbuffer::term_reader::FragmentHandler;
use crate::concurrent::logbuffer::term_scan::BlockHandler;
use crate::image::{ControlledPollAction, Image, ImageList};
use crate::utils::types::Index;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};

pub struct Subscription {
    // conductor: ClientConductor<>;
    channel: String,
    m_channelStatusId: i32,
    m_roundRobinIndex: Index,
    //todo std::size_t
    m_registrationId: i64,
    m_streamId: i32,

    m_imageList: AtomicPtr<ImageList>,
    m_isClosed: AtomicBool,
}

impl Subscription {
    /**
     * Media address for delivery to the channel.
     *
     * @return Media address for delivery to the channel.
     */
    pub fn channel(&self) -> &str {
        return &self.channel;
    }

    /**
     * Stream identity for scoping within the channel media address.
     *
     * @return Stream identity for scoping within the channel media address.
     */
    pub fn stream_id(&self) -> i32 {
        return self.m_streamId;
    }

    /**
     * Registration Id returned by Aeron::addSubscription when this Subscription was added.
     *
     * @return the registrationId of the subscription.
     */
    pub fn registration_id(&self) -> i64 {
        self.m_registrationId
    }

    /**
     * Get the counter id used to represent the channel status.
     *
     * @return the counter id used to represent the channel status.
     */
    pub fn channel_status_id(&self) -> i32 {
        self.m_channelStatusId
    }

    pub fn add_destination(&self, endpointChannel: impl AsRef<String>) {
        if self.is_closed() {
            panic!("Subscription is closed");
        }
        // self.m_conductor.addRcvDestination(m_registrationId, endpointChannel);
        // } todo
    }

    pub fn remove_destination(&self, endpointChannel: impl AsRef<String>) {
        if self.is_closed() {
            panic!("Subscription is closed"); //todo
        }

        // m_conductor.removeRcvDestination(m_registrationId, endpointChannel); todo
    }

    #[inline]
    fn load_image_list(&self) -> ImageList {
        let mut image_list = unsafe { *self.m_imageList.load(Ordering::Acquire) };
        image_list
    }

    /**
     * Poll the Image s under the subscription for having reached End of Stream.
     *
     * @param endOfStreamHandler callback for handling end of stream indication.
     * @return number of Image s that have reached End of Stream.
     * @deprecated
     */

    pub fn poll_end_of_streams(&self, endOfStreamHandler: EndOfStreamHandler) -> i32 {
        let mut numEndOfStreams = 0;

        let image_list = self.load_image_list();

        let length = image_list.length;

        for i in 0..length {
            let image = image_list.image(i);

            if image.is_end_of_stream() {
                numEndOfStreams += 1;
                endOfStreamHandler(image);
            }
        }

        return numEndOfStreams;
    }

    /**
     * Poll the {@link Image}s under the subscription for available message fragments.
     * <p>
     * Each fragment read will be a whole message if it is under MTU length. If larger than MTU then it will come
     * as a series of fragments ordered withing a session.
     *
     * @param fragmentHandler callback for handling each message fragment as it is read.
     * @param fragmentLimit   number of message fragments to limit for the poll across multiple Image s.
     * @return the number of fragments received
     *
     * @see fragment_handler_t
     */

    pub fn poll<T>(&mut self, fragmentHandler: FragmentHandler<T>, fragmentLimit: i32) -> i32 {
        let image_list = self.load_image_list();

        let length = image_list.length;

        // Image *images = imageList->m_images;
        let mut fragmentsRead = 0;

        let mut startingIndex = self.m_roundRobinIndex as isize;
        self.m_roundRobinIndex += 1;

        if startingIndex >= length {
            self.m_roundRobinIndex = 0;
            startingIndex = 0;
        }

        for i in startingIndex..length {
            if fragmentsRead < fragmentLimit {
                fragmentsRead += image_list.image(i).poll(fragmentHandler, fragmentLimit - fragmentsRead);
            }
        }

        for i in 0..startingIndex {
            if fragmentsRead < fragmentLimit {
                fragmentsRead += image_list.image(i).poll(fragmentHandler, fragmentLimit - fragmentsRead);
            }
        }

        return fragmentsRead;
    }

    /**
     * Poll in a controlled manner the Image s under the subscription for available message fragments.
     * Control is applied to fragments in the stream. If more fragments can be read on another stream
     * they will even if BREAK or ABORT is returned from the fragment handler.
     * <p>
     * Each fragment read will be a whole message if it is under MTU length. If larger than MTU then it will come
     * as a series of fragments ordered within a session.
     * <p>
     * To assemble messages that span multiple fragments then use controlled_poll_fragment_handler_t.
     *
     * @param fragmentHandler callback for handling each message fragment as it is read.
     * @param fragmentLimit   number of message fragments to limit for the poll operation across multiple Image s.
     * @return the number of fragments received
     * @see controlled_poll_fragment_handler_t
     */
    pub fn controlled_poll(&mut self, fragmentHandler: FragmentHandler<ControlledPollAction>, fragmentLimit: i32) -> i32 {
        let image_list = self.load_image_list();

        let length = image_list.length;

        // Image *images = imageList->m_images;
        let mut fragments_read = 0;

        let mut startingIndex = self.m_roundRobinIndex as isize;
        self.m_roundRobinIndex += 1;

        if startingIndex >= length {
            self.m_roundRobinIndex = 0;
            startingIndex = 0;
        }

        for i in startingIndex..length {
            if fragments_read < fragmentLimit {
                fragments_read += image_list
                    .image(i)
                    .controlled_poll(fragmentHandler, fragmentLimit - fragments_read);
            }
        }

        for i in 0..startingIndex {
            if fragments_read < fragmentLimit {
                fragments_read += image_list
                    .image(i)
                    .controlled_poll(fragmentHandler, fragmentLimit - fragments_read);
            }
        }

        return fragments_read;
    }

    /**
     * Poll the Image s under the subscription for available message fragments in blocks.
     *
     * @param blockHandler     to receive a block of fragments from each Image.
     * @param blockLengthLimit for each individual block.
     * @return the number of bytes consumed.
     */
    pub fn block_poll(&mut self, blockHandler: BlockHandler, blockLengthLimit: i32) -> i64 {
        let image_list = self.load_image_list();

        let length = image_list.length;

        let mut bytes_consumed: i64 = 0;

        for i in 0..length {
            bytes_consumed += image_list.image(i).block_poll(blockHandler, blockLengthLimit) as i64;
        }

        return bytes_consumed;
    }

    /**
     * Is the subscription connected by having at least one open image available.
     *
     * @return true if the subscription has more than one open image available.
     */
    pub fn is_connected(&self) -> bool {
        let image_list = self.load_image_list();

        let length = image_list.length;

        (0..length).any(|idx| !image_list.image(idx).is_closed())
    }

    /**
     * Count of images associated with this subscription.
     *
     * @return count of images associated with this subscription.
     */
    pub fn image_count(&self) -> isize {
        self.load_image_list().length
    }

    // /**
    //  * Return the {@link Image} associated with the given sessionId.
    //  *
    //  * This method generates a new copy of the Image overlaying the logbuffer.
    //  * It is up to the application to not use the Image if it becomes unavailable.
    //  *
    //  * @param sessionId associated with the Image.
    //  * @return Image associated with the given sessionId or nullptr if no Image exist.
    //  */
    // pub fn imageBySessionId(&self, sessionId: i32) -> Option<&mut Image> {
    //     let list = self.load_image_list();
    //
    //     let mut index = -1;
    //
    //     for i in 0..list.length {
    //         if list.image(i).session_id() == sessionId {
    //             index = i;
    //             break;
    //         }
    //     }
    //
    //     if index != -1 {
    //         Some(list.image(index))
    //     } else {
    //         None
    //     }
    // }

    // /**
    //  * Get the image at the given index from the images array.
    //  *
    //  * This is only valid until the image list changes.
    //  *
    //  * @param index in the array
    //  * @return image at given index or exception if out of range.
    //  */
    // pub fn imageAtIndex(&self, index: isize) -> &mut Image {
    //     let list = self.load_image_list();
    //     let x = list.image(index);
    //     return x;
    // }

    // /**
    //  * Get a std::vector of active {@link Image}s that match this subscription.
    //  *
    //  * @return a std::vector of active {@link Image}s that match this subscription.
    //  */
    // inline std::shared_ptr < std::vector < Image > > images() const
    // {
    // std::shared_ptr < std::vector < Image >> result(new std::vector < Image >());
    //
    // forEachImage(
    // [ & ](Image & image)
    // {
    // result -> push_back(Image(image));
    // });
    //
    // return result;
    // }

    /**
     * Has this object been closed and should no longer be used?
     *
     * @return true if it has been closed otherwise false.
     */
    pub fn is_closed(&self) -> bool {
        self.m_isClosed.load(Ordering::Acquire)
    }
}

type EndOfStreamHandler = fn(&mut Image);
