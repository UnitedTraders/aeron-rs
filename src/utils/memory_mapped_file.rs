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

use core::slice;
use std::ffi::OsString;
use std::fs::OpenOptions;
use std::ops::DerefMut;
use std::path::Path;
use std::{fs, io};

use memmap::MmapMut;

use crate::utils::types::Index;

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
    fn read<P: AsRef<Path> + Into<OsString>>(filename: P) -> Result<FileHandle, MemMappedFileError> {
        let file_path: OsString = filename.into();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
            .map_err(MemMappedFileError::IOError)?;

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

    fn fill(_fd: FileHandle, _size: usize, _value: *mut u8) -> bool {
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
        true
    }

    fn create_new<P: AsRef<Path> + Into<OsString>>(path: P, offset: Index, size: Index) -> Result<Self, MemMappedFileError> {
        let fd = FileHandle::read(path)?;
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
            eprintln!("length = {:?}", length);
        }

        let x1 = fd.mmap.deref_mut();
        let ptr = x1.as_mut_ptr();

        let x = Self {
            ptr,
            fd,
            memory_size: length,
        };

        Ok(x)
    }

    pub fn map_existing_full<P: AsRef<Path> + Into<OsString>>(
        filename: P,
        read_only: bool,
    ) -> Result<MemoryMappedFile, MemMappedFileError> {
        Self::map_existing(filename, 0, 0, read_only)
    }

    pub fn map_existing<P: AsRef<Path> + Into<OsString>>(
        filename: P,
        offset: Index,
        size: Index,
        read_only: bool,
    ) -> Result<MemoryMappedFile, MemMappedFileError> {
        let fd = FileHandle::read(filename)?;

        // FileHandle fd;
        // DWORD dwDesiredAccess = readOnly? GENERIC_READ: (GENERIC_READ | GENERIC_WRITE);
        // DWORD dwSharedMode = FILE_SHARE_READ | FILE_SHARE_WRITE;
        // fd.handle = CreateFile(filename, dwDesiredAccess, dwSharedMode, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
        //
        // if (fd.handle == INVALID_HANDLE_VALUE)
        // {
        //     throw IOException(std::string("Failed to create file: ") + filename + " " + toString(GetLastError()), SOURCEINFO);
        // }
        //
        // return MemoryMappedFile::ptr_t(new;
        // MemoryMappedFile(fd, offset, size, readOnly));
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
    use std::fs::{File, OpenOptions};
    use std::path::PathBuf;

    use super::*;

    #[test]
    #[should_panic]
    fn test_file_not_found() {
        MemoryMappedFile::create_new(Path::new("abc.file"), 0, 128).unwrap();
    }

    fn create_file() -> (File, PathBuf) {
        let tmp_dir = tempfile::tempdir().unwrap();
        let file_path = tmp_dir.path().join("mapped.file");
        let tmp_file = File::create(file_path.clone()).unwrap();
        tmp_file.set_len(10).ok();

        eprintln!("file_path = {:?}", file_path);
        tmp_file.sync_data().ok();
        (tmp_file, file_path)
    }

    #[test]
    fn test_file_size() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let file_path = tmp_dir.path().join("mapped.file");
        let tmp_file = File::create(file_path.clone()).unwrap();
        tmp_file.set_len(10).ok();

        // tmp_file.sync_data();

        let file = MemoryMappedFile::create_new(file_path, 0, 128).unwrap();
        assert_eq!(file.memory_size(), 128)
    }

    #[test]
    fn test_read_write() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let file_path = tmp_dir.path().join("mapped.file");
        eprintln!("file_path = {:?}", file_path);
        let tmp_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .create(true)
            .truncate(true)
            .open(&file_path)
            .unwrap();

        let size = 10000 as Index;
        tmp_file.set_len(size as u64).ok();

        {
            let mut file = MemoryMappedFile::create_new(file_path.clone(), 0, size as isize).unwrap();

            for n in 0..size as usize {
                let to_write = (n & 0xff) as u8;
                file.memory_mut_ptr()[n] = to_write;
            }
        }

        let file = MemoryMappedFile::map_existing_full(file_path, true).unwrap();

        for n in 0..size as usize {
            let b = file.memory_ptr();
            let option = *b.get(n).unwrap();
            assert_eq!(option, (n & 0xff) as u8)
        }
    }
}
