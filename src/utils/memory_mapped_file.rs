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

use std::fs::OpenOptions;
use std::path::Path;
use std::{fs, io};

use memmap::MmapMut;

use crate::utils::types::Index;
use core::slice;
use std::ffi::OsString;

#[derive(Debug)]
enum MemMappedFileError {
    IOError(io::Error),
}

#[derive(Debug)]
struct FileHandle {
    mmap: MmapMut,
    file_path: OsString,
}

impl FileHandle {
    fn open<P: AsRef<Path> + Into<OsString>>(filename: P, read_only: bool) -> Result<FileHandle, MemMappedFileError> {
        let file_path: OsString = filename.into();
        let file = OpenOptions::new()
            .read(true)
            .write(!read_only)
            .open(&file_path)
            .map_err(MemMappedFileError::IOError)?;

        unsafe { MmapMut::map_mut(&file) }
            .map_err(MemMappedFileError::IOError)
            .map(move |mmap| Self { mmap, file_path })
    }

    fn create<P: AsRef<Path> + Into<OsString>>(filename: P, size: Index) -> Result<FileHandle, MemMappedFileError> {
        let file_path: OsString = filename.into();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
            .map_err(MemMappedFileError::IOError)?;

        file.set_len(size as u64).map_err(MemMappedFileError::IOError)?;

        unsafe { MmapMut::map_mut(&file) }
            .map_err(MemMappedFileError::IOError)
            .map(move |mmap| Self { mmap, file_path })
    }
}

#[derive(Debug)]
struct MemoryMappedFile {
    ptr: *mut u8,
    fd: FileHandle,
    memory_size: Index,
}

impl MemoryMappedFile {
    fn page_size() -> usize {
        // ::getpagesize() todo whats that?
        0
    }

    // fn fill(fd: FileHandle, size: usize, value: *mut u8) -> bool {
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
    // true
    // }

    fn create_new<P: AsRef<Path> + Into<OsString>>(path: P, offset: Index, size: Index) -> Result<Self, MemMappedFileError> {
        let fd = FileHandle::create(path, size)?;
        //todo fill
        Self::from_file_handle(fd, offset, size, false)
    }

    fn from_file_handle(
        mut fd: FileHandle,
        offset: Index,
        mut length: Index,
        _read_only: bool,
    ) -> Result<Self, MemMappedFileError> {
        if 0 == length && 0 == offset {
            let metadata = fs::metadata(&fd.file_path).map_err(MemMappedFileError::IOError)?;

            length = metadata.len() as isize;
        }

        let mmf = Self {
            ptr: fd.mmap.as_mut_ptr(),
            fd,
            memory_size: length,
        };

        Ok(mmf)
    }

    pub fn map_existing<P: AsRef<Path> + Into<OsString>>(
        filename: P,
        read_only: bool,
    ) -> Result<MemoryMappedFile, MemMappedFileError> {
        Self::map_existing_part(filename, 0, 0, read_only)
    }

    pub fn map_existing_part<P: AsRef<Path> + Into<OsString>>(
        filename: P,
        offset: Index,
        size: Index,
        read_only: bool,
    ) -> Result<MemoryMappedFile, MemMappedFileError> {
        let fd = FileHandle::open(filename, read_only)?;

        Self::from_file_handle(fd, offset, size, read_only)
    }

    pub fn memory_mut_ptr(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.ptr, self.memory_size as usize) }
    }

    pub fn memory_ptr(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr, self.memory_size as usize) }
    }

    pub fn memory_size(&self) -> Index {
        self.memory_size
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    use crate::utils::memory_mapped_file::MemMappedFileError::IOError;
    use std::fs;
    use std::fs::{File, OpenOptions};
    use std::path::PathBuf;

    #[test]
    fn test_creating_file() {
        MemoryMappedFile::create_new(Path::new("abc.file"), 0, 128).unwrap();
    }

    #[test]
    fn test_file_size() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let file_path = tmp_dir.path().join("mapped.file");
        let tmp_file = File::create(file_path.clone()).unwrap();
        tmp_file.set_len(10).unwrap();

        // tmp_file.sync_data();

        let file = MemoryMappedFile::create_new(file_path, 0, 128).unwrap();
        assert_eq!(file.memory_size(), 128)
    }

    #[test]
    fn test_read_write() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let file_path = tmp_dir.path().join("mapped.file");
        let tmp_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
            .unwrap();

        let size = 10000 as Index;
        tmp_file.set_len(size as u64).unwrap();

        {
            let mut file = MemoryMappedFile::create_new(file_path.clone(), 0, size as isize).unwrap();

            for n in 0..size as usize {
                let to_write = (n & 0xff) as u8;
                file.memory_mut_ptr()[n] = to_write;
            }
        }

        let file = MemoryMappedFile::map_existing(file_path, false).unwrap();

        for n in 0..size as usize {
            let b = file.memory_ptr();
            let option = *b.get(n).unwrap();
            assert_eq!(option, (n & 0xff) as u8)
        }
    }
}
