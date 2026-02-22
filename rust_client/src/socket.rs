// Copyright 2024-2026 David Allison
// All Rights Reserved
// See LICENSE file for licensing information.

use crate::error::{Result, SubspaceError};
use crate::proto;
use nix::sys::socket::{
    connect, recvmsg, sendmsg, socket, AddressFamily, ControlMessage, ControlMessageOwned,
    MsgFlags, SockFlag, SockType, UnixAddr,
};
use prost::Message;
use std::io::{IoSlice, IoSliceMut};
use std::os::fd::AsRawFd;
use std::os::unix::io::RawFd;

const MAX_FDS_PER_MSG: usize = 252;

pub struct SocketConnection {
    fd: RawFd,
}

impl SocketConnection {
    pub fn connect(server_socket: &str) -> Result<Self> {
        let owned_fd = socket(
            AddressFamily::Unix,
            SockType::Stream,
            SockFlag::empty(),
            None,
        )?;
        let fd = owned_fd.as_raw_fd();

        // Linux abstract namespace: pad to full sun_path size (108 bytes) to
        // match the C++ server which binds with sizeof(sockaddr_un).  The kernel
        // distinguishes abstract sockets by the full address length, so the name
        // must include the trailing null-byte padding.
        #[cfg(target_os = "linux")]
        let addr = {
            const SUN_PATH_LEN: usize = 108;
            let mut padded = vec![0u8; SUN_PATH_LEN - 1];
            let name_bytes = server_socket.as_bytes();
            padded[..name_bytes.len()].copy_from_slice(name_bytes);
            UnixAddr::new_abstract(&padded)?
        };

        #[cfg(not(target_os = "linux"))]
        let addr = UnixAddr::new(server_socket)?;

        connect(fd, &addr)?;
        std::mem::forget(owned_fd);
        Ok(Self { fd })
    }

    pub fn fd(&self) -> RawFd {
        self.fd
    }

    pub fn send_request(&self, req: &proto::Request) -> Result<()> {
        let msg_len = req.encoded_len();
        let mut buf = vec![0u8; 4 + msg_len];

        // Length prefix in network byte order (big-endian).
        let len_be = (msg_len as u32).to_be_bytes();
        buf[..4].copy_from_slice(&len_be);
        req.encode(&mut &mut buf[4..])?;

        send_fully(self.fd, &buf)?;
        Ok(())
    }

    pub fn receive_response(&self) -> Result<(proto::Response, Vec<RawFd>)> {
        // 1. Read length-prefixed protobuf response.
        let mut len_buf = [0u8; 4];
        recv_fully(self.fd, &mut len_buf)?;
        let length = u32::from_be_bytes(len_buf) as usize;

        let mut data = vec![0u8; length];
        recv_fully(self.fd, &mut data)?;

        let response = proto::Response::decode(&data[..])?;

        // 2. Receive file descriptors via SCM_RIGHTS.
        let fds = self.receive_fds()?;

        Ok((response, fds))
    }

    fn receive_fds(&self) -> Result<Vec<RawFd>> {
        let mut all_fds: Vec<RawFd> = Vec::new();
        let cmsg_size = unsafe {
            libc::CMSG_SPACE((MAX_FDS_PER_MSG * std::mem::size_of::<RawFd>()) as u32) as usize
        };
        let mut cmsg_buf = vec![0u8; cmsg_size];

        loop {
            let mut total_fds_buf = [0u8; 4];
            let (bytes, cmsgs) = {
                let mut iov = [IoSliceMut::new(&mut total_fds_buf)];
                let msg =
                    recvmsg::<()>(self.fd, &mut iov, Some(&mut cmsg_buf), MsgFlags::empty())?;
                let mut fds_from_cmsgs = Vec::new();
                for cmsg in msg.cmsgs()? {
                    if let ControlMessageOwned::ScmRights(fds) = cmsg {
                        fds_from_cmsgs.extend_from_slice(&fds);
                    }
                }
                (msg.bytes, fds_from_cmsgs)
            };

            if bytes == 0 {
                return Err(SubspaceError::Internal(
                    "EOF while reading file descriptors".into(),
                ));
            }

            all_fds.extend_from_slice(&cmsgs);
            let total_fds = i32::from_ne_bytes(total_fds_buf) as usize;

            if all_fds.len() >= total_fds {
                break;
            }
        }

        Ok(all_fds)
    }
}

impl Drop for SocketConnection {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}

fn send_fully(fd: RawFd, data: &[u8]) -> Result<()> {
    let mut offset = 0;
    while offset < data.len() {
        let iov = [IoSlice::new(&data[offset..])];
        let sent = sendmsg::<()>(fd, &iov, &[], MsgFlags::empty(), None)?;
        if sent == 0 {
            return Err(SubspaceError::Internal("Socket send returned 0".into()));
        }
        offset += sent;
    }
    Ok(())
}

fn recv_fully(fd: RawFd, buf: &mut [u8]) -> Result<()> {
    let mut offset = 0;
    while offset < buf.len() {
        let mut iov = [IoSliceMut::new(&mut buf[offset..])];
        let msg = recvmsg::<()>(fd, &mut iov, None, MsgFlags::empty())?;
        if msg.bytes == 0 {
            return Err(SubspaceError::Internal("Socket closed unexpectedly".into()));
        }
        offset += msg.bytes;
    }
    Ok(())
}

/// Send file descriptors via SCM_RIGHTS (used by bridge publishers, not typical client usage).
#[allow(dead_code)]
pub fn send_fds(fd: RawFd, fds: &[RawFd]) -> Result<()> {
    let mut remaining = fds.len();
    let mut first_fd = 0;
    let total_fds = fds.len() as i32;

    loop {
        let batch_size = remaining.min(MAX_FDS_PER_MSG);
        let batch = &fds[first_fd..first_fd + batch_size];
        let total_buf = total_fds.to_ne_bytes();
        let iov = [IoSlice::new(&total_buf)];
        let cmsg = [ControlMessage::ScmRights(batch)];

        sendmsg::<()>(fd, &iov, &cmsg, MsgFlags::empty(), None)?;

        remaining -= batch_size;
        first_fd += batch_size;
        if remaining == 0 {
            break;
        }
    }

    // Must send at least one message, even with 0 fds.
    if fds.is_empty() {
        let total_buf = 0i32.to_ne_bytes();
        let iov = [IoSlice::new(&total_buf)];
        sendmsg::<()>(fd, &iov, &[], MsgFlags::empty(), None)?;
    }

    Ok(())
}
