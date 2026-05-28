// Copyright 2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

use crate::error::{Result, SubspaceError};
use crate::proto;
use crate::syscall_shim::{shim_close, shim_ftruncate, shim_open, shim_read, shim_write};
use nix::fcntl::OFlag;
use nix::sys::stat::Mode;
use prost::Message as ProstMessage;
use std::os::unix::io::RawFd;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub struct SplitBufferMetadata {
    pub channel_name: String,
    pub session_id: u64,
    pub buffer_index: u32,
    pub slot_id: u32,
    pub is_prefix: bool,
    pub full_size: u64,
    pub allocation_size: u64,
    pub handle: u64,
    pub shadow_file: String,
    pub object_name: String,
}

#[derive(Debug, Clone, Copy)]
pub struct SplitBufferMapping {
    pub handle: u64,
    pub address: *mut u8,
    pub size: usize,
    pub private_data: usize,
}

impl Default for SplitBufferMapping {
    fn default() -> Self {
        Self {
            handle: 0,
            address: std::ptr::null_mut(),
            size: 0,
            private_data: 0,
        }
    }
}

pub type SplitBufferAllocateCallback =
    Arc<dyn Fn(&SplitBufferMetadata) -> Result<SplitBufferMapping> + Send + Sync>;
pub type SplitBufferMapCallback =
    Arc<dyn Fn(&SplitBufferMetadata) -> Result<SplitBufferMapping> + Send + Sync>;
pub type SplitBufferUnmapCallback =
    Arc<dyn Fn(&SplitBufferMetadata, &SplitBufferMapping) -> Result<()> + Send + Sync>;
pub type SplitBufferFreeCallback =
    Arc<dyn Fn(&SplitBufferMetadata, &SplitBufferMapping) -> Result<()> + Send + Sync>;

#[derive(Clone, Default)]
pub struct SplitBufferCallbacks {
    pub allocate: Option<SplitBufferAllocateCallback>,
    pub map: Option<SplitBufferMapCallback>,
    pub unmap: Option<SplitBufferUnmapCallback>,
    pub free: Option<SplitBufferFreeCallback>,
}

impl std::fmt::Debug for SplitBufferCallbacks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SplitBufferCallbacks")
            .field("allocate", &self.allocate.is_some())
            .field("map", &self.map.is_some())
            .field("unmap", &self.unmap.is_some())
            .field("free", &self.free.is_some())
            .finish()
    }
}

impl SplitBufferMetadata {
    pub fn to_proto(&self, allocator: &str) -> proto::ClientBufferHandleMetadataProto {
        let allocator = match allocator {
            "split_shm" => proto::ClientBufferAllocator::SplitShm as i32,
            "split_callback" => proto::ClientBufferAllocator::SplitCallback as i32,
            "split_buffer_free_test" => {
                proto::ClientBufferAllocator::SplitBufferFreeTest as i32
            }
            _ => proto::ClientBufferAllocator::Unspecified as i32,
        };
        proto::ClientBufferHandleMetadataProto {
            channel_name: self.channel_name.clone(),
            session_id: self.session_id,
            buffer_index: self.buffer_index,
            slot_id: self.slot_id,
            is_prefix: self.is_prefix,
            full_size: self.full_size,
            allocation_size: self.allocation_size,
            handle: self.handle,
            shadow_file: self.shadow_file.clone(),
            object_name: self.object_name.clone(),
            allocator,
        }
    }
}

impl From<proto::ClientBufferHandleMetadataProto> for SplitBufferMetadata {
    fn from(proto: proto::ClientBufferHandleMetadataProto) -> Self {
        Self {
            channel_name: proto.channel_name,
            session_id: proto.session_id,
            buffer_index: proto.buffer_index,
            slot_id: proto.slot_id,
            is_prefix: proto.is_prefix,
            full_size: proto.full_size,
            allocation_size: proto.allocation_size,
            handle: proto.handle,
            shadow_file: proto.shadow_file,
            object_name: proto.object_name,
        }
    }
}

pub fn page_aligned_size(size: u64) -> u64 {
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    let page_size = if page_size <= 0 {
        4096
    } else {
        page_size as u64
    };
    (size + page_size - 1) & !(page_size - 1)
}

pub fn split_buffer_object_name(shadow_file: &str) -> String {
    format!("subspace_sb_{:016x}", stable_hash64(shadow_file))
}

fn stable_hash64(value: &str) -> u64 {
    let mut hash = 1_469_598_103_934_665_603u64;
    for c in value.bytes() {
        hash ^= c as u64;
        hash = hash.wrapping_mul(1_099_511_628_211u64);
    }
    hash
}

pub fn write_split_buffer_metadata_file(metadata: &SplitBufferMetadata) -> Result<()> {
    let proto = metadata.to_proto("");
    let mut contents = Vec::new();
    proto.encode(&mut contents)?;

    let tmp = format!("{}.tmp", metadata.shadow_file);
    let fd = shim_open(
        &tmp,
        OFlag::O_WRONLY | OFlag::O_CREAT | OFlag::O_TRUNC,
        Mode::from_bits_truncate(0o666),
    )?;
    let write_result = write_all(fd, &contents);
    let close_result = shim_close(fd);
    if let Err(e) = write_result {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    if close_result == -1 {
        let _ = std::fs::remove_file(&tmp);
        return Err(SubspaceError::Io(std::io::Error::last_os_error()));
    }
    std::fs::rename(&tmp, &metadata.shadow_file)?;
    Ok(())
}

pub fn read_split_buffer_metadata_file(shadow_file: &str) -> Result<SplitBufferMetadata> {
    let mut last_error = None;
    for _ in 0..100 {
        match read_split_buffer_metadata_file_once(shadow_file) {
            Ok(metadata) => return Ok(metadata),
            Err(e) => {
                last_error = Some(e);
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
    }
    Err(last_error.unwrap_or_else(|| {
        SubspaceError::Internal(format!(
            "Failed to read split buffer metadata {shadow_file}"
        ))
    }))
}

fn read_split_buffer_metadata_file_once(shadow_file: &str) -> Result<SplitBufferMetadata> {
    let fd = shim_open(shadow_file, OFlag::O_RDONLY, Mode::empty())?;
    let result = (|| -> Result<SplitBufferMetadata> {
        let stat = crate::syscall_shim::shim_fstat(fd)?;
        let mut contents = vec![0u8; stat.st_size as usize];
        let mut offset = 0usize;
        while offset < contents.len() {
            let n = shim_read(
                fd,
                contents[offset..].as_mut_ptr() as *mut libc::c_void,
                contents.len() - offset,
            );
            if n < 0 {
                return Err(SubspaceError::Io(std::io::Error::last_os_error()));
            }
            if n == 0 {
                break;
            }
            offset += n as usize;
        }
        let proto = proto::ClientBufferHandleMetadataProto::decode(&contents[..offset])?;
        Ok(proto.into())
    })();
    shim_close(fd);
    result
}

fn write_all(fd: RawFd, contents: &[u8]) -> Result<()> {
    let mut offset = 0usize;
    while offset < contents.len() {
        let n = shim_write(
            fd,
            contents[offset..].as_ptr() as *const libc::c_void,
            contents.len() - offset,
        );
        if n < 0 {
            return Err(SubspaceError::Io(std::io::Error::last_os_error()));
        }
        offset += n as usize;
    }
    Ok(())
}

pub fn create_split_shared_memory_buffer(metadata: &SplitBufferMetadata) -> Result<Option<RawFd>> {
    let fd = split_shm_open(
        &metadata.object_name,
        OFlag::O_RDWR | OFlag::O_CREAT | OFlag::O_EXCL,
        Mode::from_bits_truncate(0o666),
    );
    let fd = match fd {
        Ok(fd) => fd,
        Err(nix::errno::Errno::EEXIST) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let allocation_size = if metadata.allocation_size == 0 {
        page_aligned_size(metadata.full_size)
    } else {
        metadata.allocation_size
    };
    if shim_ftruncate(fd, allocation_size as libc::off_t) == -1 {
        let _ = shim_close(fd);
        return Err(SubspaceError::Io(std::io::Error::last_os_error()));
    }
    Ok(Some(fd))
}

pub fn open_split_shared_memory_buffer(metadata: &SplitBufferMetadata) -> Result<RawFd> {
    Ok(split_shm_open(
        &metadata.object_name,
        OFlag::O_RDWR,
        Mode::empty(),
    )?)
}

#[cfg(target_os = "linux")]
fn split_shm_open(name: &str, oflag: OFlag, mode: Mode) -> nix::Result<RawFd> {
    shim_open(&format!("/dev/shm/{name}"), oflag, mode)
}

#[cfg(not(target_os = "linux"))]
fn split_shm_open(name: &str, oflag: OFlag, mode: Mode) -> nix::Result<RawFd> {
    crate::syscall_shim::shim_shm_open(name, oflag, mode)
}
