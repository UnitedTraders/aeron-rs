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
use std::{
    ffi::OsString,
    fs::{self, OpenOptions},
    path::Path,
};

use memmap::MmapMut;

use crate::concurrent::atomic_buffer::AtomicBuffer;
use crate::utils::{errors::AeronError, types::Index};

#[derive(Debug)]
pub struct FileHandle {
    mmap: MmapMut,
    file_path: OsString,
}

impl FileHandle {
    fn open<P: AsRef<Path> + Into<OsString>>(filename: P, read_only: bool) -> Result<FileHandle, AeronError> {
        let file_path: OsString = filename.into();
        let file = OpenOptions::new()
            .read(true)
            .write(!read_only)
            .open(&file_path)
            .map_err(AeronError::MemMappedFileError)?;

        unsafe { MmapMut::map_mut(&file) }
            .map_err(AeronError::MemMappedFileError)
            .map(move |mmap| Self { mmap, file_path })
    }

    fn create<P: AsRef<Path> + Into<OsString>>(filename: P, size: Index) -> Result<FileHandle, AeronError> {
        let file_path: OsString = filename.into();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
            .map_err(AeronError::MemMappedFileError)?;

        file.set_len(size as u64).map_err(AeronError::MemMappedFileError)?;

        unsafe { MmapMut::map_mut(&file) }
            .map_err(AeronError::MemMappedFileError)
            .map(move |mmap| Self { mmap, file_path })
    }
}

#[derive(Debug)]
pub struct MemoryMappedFile {
    ptr: *mut u8,
    fd: FileHandle,
    memory_size: Index,
}

// Mutable access to the ptr goes through creation of AtomicBuffer which is thread safe.
// There is memory_mut_ptr() method but its only used for tests.
unsafe impl Send for MemoryMappedFile {}
unsafe impl Sync for MemoryMappedFile {}

impl MemoryMappedFile {
    #[allow(dead_code)]
    fn page_size() -> usize {
        // ::getpagesize() todo whats that?
        0
    }

    pub fn get_file_size<P: AsRef<Path>>(file: P) -> Result<u64, AeronError> {
        let metadata = fs::metadata(file).map_err(AeronError::MemMappedFileError)?;
        Ok(metadata.len())
    }

    pub fn create_new<P: AsRef<Path> + Into<OsString>>(path: P, offset: Index, size: Index) -> Result<Self, AeronError> {
        let fd = FileHandle::create(path, size)?;
        //todo fill
        Self::from_file_handle(fd, offset, size, false)
    }

    fn from_file_handle(mut fd: FileHandle, offset: Index, mut length: Index, _read_only: bool) -> Result<Self, AeronError> {
        if 0 == length && 0 == offset {
            length = Self::get_file_size(&fd.file_path)? as Index;
        }

        let mmf = Self {
            ptr: fd.mmap.as_mut_ptr(),
            fd,
            memory_size: length,
        };

        Ok(mmf)
    }

    pub fn map_existing<P: AsRef<Path> + Into<OsString>>(filename: P, read_only: bool) -> Result<MemoryMappedFile, AeronError> {
        Self::map_existing_part(filename, 0, 0, read_only)
    }

    pub fn map_existing_part<P: AsRef<Path> + Into<OsString>>(
        filename: P,
        offset: Index,
        size: Index,
        read_only: bool,
    ) -> Result<MemoryMappedFile, AeronError> {
        let fd = FileHandle::open(filename, read_only)?;

        Self::from_file_handle(fd, offset, size, read_only)
    }

    #[cfg(test)]
    pub fn memory_mut_ptr(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.ptr, self.memory_size as usize) }
    }

    pub fn memory_ptr(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr, self.memory_size as usize) }
    }

    pub fn memory_size(&self) -> Index {
        self.memory_size
    }

    pub fn atomic_buffer(&self, offset: Index, size: Index) -> AtomicBuffer {
        unsafe { AtomicBuffer::new(self.ptr.offset(offset as isize), size) }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{File, OpenOptions};
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn test_creating_file() {
        MemoryMappedFile::create_new(Path::new("abc.file"), 0, 128).unwrap();
    }

    #[allow(dead_code)]
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

        let size = 10000;
        tmp_file.set_len(size as u64).unwrap();

        {
            let mut file = MemoryMappedFile::create_new(file_path.clone(), 0, size).unwrap();

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
