use crate::concurrent::logbuffer::term_reader::FragmentHandler;
use crate::concurrent::logbuffer::term_scan::BlockHandler;
use crate::image::{ControlledPollAction, Image, ImageList};
use crate::utils::types::Index;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::Arc;

pub struct Subscription {
    // conductor: ClientConductor<>;
    channel: String,
    channel_status_id: i32,
    round_robin_index: Index,
    //todo std::size_t
    registration_id: i64,
    stream_id: i32,

    image_list: AtomicPtr<ImageList>,
    is_closed: AtomicBool,
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
        return self.stream_id;
    }

    /**
     * Registration Id returned by Aeron::addSubscription when this Subscription was added.
     *
     * @return the registrationId of the subscription.
     */
    pub fn registration_id(&self) -> i64 {
        self.registration_id
    }

    /**
     * Get the counter id used to represent the channel status.
     *
     * @return the counter id used to represent the channel status.
     */
    pub fn channel_status_id(&self) -> i32 {
        self.channel_status_id
    }

    pub fn add_destination(&self, _endpoint_channel: impl AsRef<String>) {
        if self.is_closed() {
            panic!("Subscription is closed");
        }
        // self.m_conductor.addRcvDestination(registration_id, endpoint_channel);
        // } todo
    }

    pub fn remove_destination(&self, _endpoint_channel: impl AsRef<String>) {
        if self.is_closed() {
            panic!("Subscription is closed"); //todo
        }

        // m_conductor.removeRcvDestination(registration_id, endpoint_channel); todo
    }

    #[inline]
    fn load_image_list(&self) -> ImageList {
        let image_list = unsafe { *self.image_list.load(Ordering::Acquire) };
        image_list
    }

    /**
     * Poll the Image s under the subscription for having reached End of Stream.
     *
     * @param end_of_stream_handler callback for handling end of stream indication.
     * @return number of Image s that have reached End of Stream.
     * @deprecated
     */

    pub fn poll_end_of_streams(&self, end_of_stream_handler: EndOfStreamHandler) -> i32 {
        let mut num_end_of_streams = 0;

        let image_list = self.load_image_list();

        let length = image_list.length;

        for i in 0..length {
            let image = image_list.image(i);

            if image.is_end_of_stream() {
                num_end_of_streams += 1;
                end_of_stream_handler(image);
            }
        }

        return num_end_of_streams;
    }

    /**
     * Poll the {@link Image}s under the subscription for available message fragments.
     * <p>
     * Each fragment read will be a whole message if it is under MTU length. If larger than MTU then it will come
     * as a series of fragments ordered withing a session.
     *
     * @param fragment_handler callback for handling each message fragment as it is read.
     * @param fragment_limit   number of message fragments to limit for the poll across multiple Image s.
     * @return the number of fragments received
     *
     * @see fragment_handler_t
     */

    pub fn poll<T>(&mut self, fragment_handler: FragmentHandler<T>, fragment_limit: i32) -> i32 {
        let image_list = self.load_image_list();

        let length = image_list.length;

        // Image *images = imageList->m_images;
        let mut fragments_read = 0;

        let mut starting_index = self.round_robin_index as isize;
        self.round_robin_index += 1;

        if starting_index >= length {
            self.round_robin_index = 0;
            starting_index = 0;
        }

        for i in starting_index..length {
            if fragments_read < fragment_limit {
                fragments_read += image_list.image(i).poll(fragment_handler, fragment_limit - fragments_read);
            }
        }

        for i in 0..starting_index {
            if fragments_read < fragment_limit {
                fragments_read += image_list.image(i).poll(fragment_handler, fragment_limit - fragments_read);
            }
        }

        return fragments_read;
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
     * @param fragment_handler callback for handling each message fragment as it is read.
     * @param fragment_limit   number of message fragments to limit for the poll operation across multiple Image s.
     * @return the number of fragments received
     * @see controlled_poll_fragment_handler_t
     */
    pub fn controlled_poll(&mut self, fragment_handler: FragmentHandler<ControlledPollAction>, fragment_limit: i32) -> i32 {
        let image_list = self.load_image_list();

        let length = image_list.length;

        // Image *images = imageList->m_images;
        let mut fragments_read = 0;

        let mut starting_index = self.round_robin_index as isize;
        self.round_robin_index += 1;

        if starting_index >= length {
            self.round_robin_index = 0;
            starting_index = 0;
        }

        for i in starting_index..length {
            if fragments_read < fragment_limit {
                fragments_read += image_list
                    .image(i)
                    .controlled_poll(fragment_handler, fragment_limit - fragments_read);
            }
        }

        for i in 0..starting_index {
            if fragments_read < fragment_limit {
                fragments_read += image_list
                    .image(i)
                    .controlled_poll(fragment_handler, fragment_limit - fragments_read);
            }
        }

        return fragments_read;
    }

    /**
     * Poll the Image s under the subscription for available message fragments in blocks.
     *
     * @param block_handler     to receive a block of fragments from each Image.
     * @param block_length_limit for each individual block.
     * @return the number of bytes consumed.
     */
    pub fn block_poll(&mut self, block_handler: BlockHandler, block_length_limit: i32) -> i64 {
        let image_list = self.load_image_list();

        let length = image_list.length;

        let mut bytes_consumed: i64 = 0;

        for i in 0..length {
            bytes_consumed += image_list.image(i).block_poll(block_handler, block_length_limit) as i64;
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
        self.is_closed.load(Ordering::Acquire)
    }

    // FIXME: stubs for compilation of other files only

    // Adds image to the subscription and returns ImageList
    // as it was just before adding this Image
    pub fn add_image(&mut self, _image: Arc<Image>) -> ImageList {
        ImageList {
            ptr: 0 as *mut Image,
            length: 0,
        }
    }

    // Removes image with given correlation_id and returns old ImageArray (as of before removal)
    // and index of removed element. So effectively it return the Image deleted.
    // Returns None if Image was not removed (e.g. was not found).
    pub fn remove_image(&mut self, _correlation_id: i64) -> Option<(ImageList, Index)> {
        None
    }

    // Removes all images and returns old ImageArray if subscription is not closed.
    // Returns None if subscription is closed.
    pub fn close_and_remove_images(&mut self) -> Option<ImageList> {
        None
    }
}

type EndOfStreamHandler = fn(&mut Image);
