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

use std::fs::File;
use std::io;
use std::path::Path;

use memmap::Mmap;

use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::utils::types::Index;

#[derive(Debug)]
enum MemMappedFileError {
    IOError(io::Error),
}

struct FileHandle {
    handle: usize,
    mmap: Mmap,
}

#[derive(Debug)]
struct MemoryMappedFile {
    memory: *mut u8,
    memory_size: Index,
}

impl MemoryMappedFile {
    fn page_size() -> usize {
        // ::getpagesize() todo whats that?
        0
    }

    fn fill(fd: FileHandle, size: usize, value: *mut u8) -> bool {
        // std::unique_ptr < uint8_t[] > buffer(new uint8_t[m_page_size]);
        // memset(buffer.get(), value, m_page_size);

        // while size >= Self::page_size() {
        //     if (static_cast < size_t > (write(fd.handle, buffer.get(), m_page_size)) != m_page_size)
        //     {
        //         return false;
        //     }
        //
        //     size -= m_page_size;
        // }
        //
        // if size {
        //     if write(fd.handle, buffer.get(), size)) != size
        //     {
        //         return false;
        //     }
        // }
        return true;
    }

    fn create_new<P: AsRef<Path>>(path: P, offset: Index, size: Index) -> Result<Self, MemMappedFileError> {
        let file = File::open(path).map_err(|err| MemMappedFileError::IOError(err))?;

        let mmap = unsafe { Mmap::map(&file).map_err(|err| MemMappedFileError::IOError(err))? };
        let fd = FileHandle { handle: 0, mmap };
        // self

        /*     FileHandle fd;
                fd.handle = open(filename, O_RDWR | O_CREAT, 0666);

                if (fd.handle < 0)
                {
                    throw IOException(std::string("failed to create file: ") + filename, SOURCEINFO);
                }

                OnScopeExit tidy([&]()
                {
                    close(fd.handle);
                });

                if (!fill(fd, size, 0))
                {
                    throw IOException(std::string("failed to write to file: ") + filename, SOURCEINFO);
                }
        */
        return Self::from_file_handle(fd, offset, size, false);
    }

    fn from_file_handle(fd: FileHandle, offset: Index, length: Index, read_only: bool) -> Result<Self, MemMappedFileError> {
        if 0 == length && 0 == offset {
            // struct stat statInfo;
            // ::fstat(fd.handle, &statInfo);
            // length = statInfo.st_size;
        }

        Ok(Self {
            memory: Self::do_mapping(length, fd, offset, read_only),
            memory_size: length,
        })
    }

    fn do_mapping(length: Index, fd: FileHandle, offset: Index, read_only: bool) -> *mut u8 {
        // void * memory = ::mmap(
        //     NULL,
        //     length,
        //     (readOnly? PROT_READ: (PROT_READ | PROT_WRITE)),
        // MAP_SHARED,
        // fd.handle,
        // static_cast < off_t > (offset));
        //
        // if (MAP_FAILED == memory)
        // {
        //     throw IOException("failed to Memory Map File", SOURCEINFO);
        // }
        //
        // return static_cast < uint8_t * > (memory);

        let ab = AtomicBuffer::wrap_slice(&mut [0]);
        ab.buffer() //todo
    }

    fn memory_ptr(&self) -> *const u8 {
        self.memory
    }

    fn memory_size(&self) -> Index {
        self.memory_size
    }
}

#[cfg(test)]
mod tests {
    use std::io::{ErrorKind, Write};

    use super::*;
    use crate::utils::memory_mapped_file::MemMappedFileError::IOError;
    use std::fs;
    use std::path::PathBuf;

    #[test]
    #[should_panic]
    fn test_file_not_found() {
        MemoryMappedFile::create_new(Path::new("abc.file"), 0, 128).unwrap();
    }

    fn create_file() -> (File, PathBuf) {
        let tmp_dir = tempfile::tempdir().unwrap();
        let file_path = tmp_dir.path().join("mapped.file");
        let mut tmp_file = File::create(file_path.clone()).unwrap();
        tmp_file.set_len(10);

        eprintln!("file_path = {:?}", file_path);
        tmp_file.sync_data();
        (tmp_file, file_path)
    }

    #[test]
    fn test_file_size() {
        // let (mut tmp_file, file_path) = create_file();

        let tmp_dir = tempfile::tempdir().unwrap();
        let file_path = tmp_dir.path().join("mapped.file");
        let mut tmp_file = File::create(file_path.clone()).unwrap();
        tmp_file.set_len(10);

        // tmp_file.sync_data();

        let file = MemoryMappedFile::create_new(file_path, 0, 128).unwrap();
        assert_eq!(file.memory_size(), 128)
    }

    #[test]
    fn test_read_write() {}
}
