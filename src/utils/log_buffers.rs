use std::ffi::OsString;
use std::path::Path;

use crate::{
    concurrent::{
        atomic_buffer::AtomicBuffer,
        logbuffer::log_buffer_descriptor::{self, check_page_size, check_term_length, page_size, term_length, PARTITION_COUNT},
    },
    ttrace,
    utils::{errors::AeronError, memory_mapped_file::MemoryMappedFile, types::Index},
};

#[allow(dead_code)]
pub struct LogBuffers {
    memory_mapped_file: Option<MemoryMappedFile>,
    buffers: [AtomicBuffer; log_buffer_descriptor::PARTITION_COUNT as usize + 1],
}

impl LogBuffers {
    /// # Safety
    ///
    /// LogBuffer is created internally by Publication and Image and not designed to
    /// be created by application level code.
    pub unsafe fn new(address: *mut u8, log_length: isize, term_length: i32) -> Self {
        assert_eq!(log_buffer_descriptor::PARTITION_COUNT, 3);

        Self {
            memory_mapped_file: None,
            buffers: [
                AtomicBuffer::new(address, term_length),
                AtomicBuffer::new(address.offset(term_length as isize), term_length),
                AtomicBuffer::new(address.offset(2 * term_length as isize), term_length),
                AtomicBuffer::new(
                    address.offset(log_length - log_buffer_descriptor::LOG_META_DATA_LENGTH as isize),
                    log_buffer_descriptor::LOG_META_DATA_LENGTH,
                ),
            ],
        }
    }

    pub(crate) fn from_existing<P: std::fmt::Display + AsRef<Path> + Into<OsString>>(
        file_path: P,
        pre_touch: bool,
    ) -> Result<Self, AeronError> {
        assert_eq!(log_buffer_descriptor::PARTITION_COUNT, 3);

        ttrace!("from_existing: file_path {}, pre_touch {}", &file_path, pre_touch);

        let log_len = MemoryMappedFile::get_file_size(&file_path)?;

        let memory_mapped_file = MemoryMappedFile::map_existing(file_path, false).expect("todo");

        let meta_buffer = memory_mapped_file.atomic_buffer(
            (log_len as Index) - log_buffer_descriptor::LOG_META_DATA_LENGTH,
            log_buffer_descriptor::LOG_META_DATA_LENGTH,
        );

        let term_length = term_length(&meta_buffer) as Index;
        let page_size = page_size(&meta_buffer);

        check_term_length(term_length)?;
        check_page_size(page_size)?;

        let mut buffers: Vec<AtomicBuffer> = Vec::with_capacity((PARTITION_COUNT + 1) as usize);

        for i in 0..PARTITION_COUNT {
            let buffer = memory_mapped_file.atomic_buffer(i * term_length, term_length);

            if pre_touch {
                let mut offset = 0;
                while offset < term_length {
                    let _ignored = buffer.get::<i32>(offset);
                    offset += page_size;
                }
            }

            buffers.push(buffer)
        }

        buffers.push(meta_buffer);

        ttrace!("from_existing: file mapped successfully, term_length {}", term_length);

        Ok(Self {
            memory_mapped_file: Some(memory_mapped_file),
            buffers: [
                *buffers.get(0).expect("Log buffers get(0) failed"),
                *buffers.get(1).expect("Log buffers get(1) failed"),
                *buffers.get(2).expect("Log buffers get(2) failed"),
                *buffers.get(3).expect("Log buffers get(3) failed"),
            ],
        })
    }

    pub fn atomic_buffer(&self, index: Index) -> AtomicBuffer {
        self.buffers[index as usize]
    }
}
