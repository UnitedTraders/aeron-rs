use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::concurrent::logbuffer::log_buffer_descriptor;
use crate::concurrent::logbuffer::log_buffer_descriptor::{
    check_page_size, check_term_length, page_size, term_length, PARTITION_COUNT,
};
use crate::utils::errors::AeronError;
use crate::utils::memory_mapped_file::MemoryMappedFile;
use crate::utils::types::Index;
use std::ffi::OsString;
use std::path::Path;

struct LogBuffers {
    memory_mapped_file: MemoryMappedFile,
    buffers: Vec<AtomicBuffer>,
}

impl LogBuffers {
    pub fn from_existing<P: AsRef<Path> + Into<OsString>>(file_path: P) -> Result<Self, AeronError> {
        let log_len = MemoryMappedFile::file_size(&file_path).map_err(AeronError::MemMappedFileError)?;

        let memory_mapped_file = MemoryMappedFile::map_existing(file_path, false).expect("todo");
        let atomic_buffer = memory_mapped_file.atomic_buffer(0, log_len as Index);

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

            buffers.push(buffer)
        }

        buffers.push(meta_buffer);

        Ok(Self {
            memory_mapped_file,
            buffers,
        })
    }

    pub fn atomic_buffer(&self, _index: Index) -> AtomicBuffer {
        self.memory_mapped_file.atomic_buffer(0, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    // #[test]
    fn test_new() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let file_path = tmp_dir.path().join("mapped.file");
        let tmp_file = File::create(file_path.clone()).unwrap();

        tmp_file.set_len(10).unwrap();

        // tmp_file.sync_data();

        let buffers = LogBuffers::from_existing(file_path).unwrap();
        let buffer = buffers.atomic_buffer(0);

        // assert_eq!(file.memory_size(), 128);

        assert_eq!(buffers.atomic_buffer(0).capacity(), 128)
    }
}
