#[macro_use]
extern crate log;

use ahash::{AHashMap, AHashSet};
use anyhow::anyhow;
use aws_sdk_s3::config::{Credentials, SharedCredentialsProvider};
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::Client;
use aws_types::region::Region;
use aws_types::SdkConfig;
use bytes::{Buf, Bytes, };
use clap::Parser;
use fuse3::path::prelude::*;
use fuse3::{Errno, MountOptions, Result};
use futures_util::stream::{Empty, Iter};
use futures_util::{stream, TryFutureExt};
use log::LevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::config::{Appender, Root};
use log4rs::encode::pattern::PatternEncoder;
use rust_lapper::{Interval, Lapper};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::io::Read;
use std::num::NonZeroU32;
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::vec::IntoIter;
use std::{io, task};
use std::net::SocketAddr;
use std::task::Poll;
use aws_sdk_s3::config::http::HttpResponse;
use aws_sdk_s3::operation::delete_object::{DeleteObjectError, DeleteObjectOutput};
use aws_sdk_s3::operation::delete_objects::{DeleteObjectsError, DeleteObjectsOutput};
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use aws_smithy_runtime::client::http::hyper_014::HyperClientBuilder;
use futures_util::future::MapOk;
use hyper::client::connect::dns::{GaiAddrs, GaiFuture, GaiResolver, Name};
use hyper::client::HttpConnector;
use hyper::service::Service;
use mimalloc::MiMalloc;
use parking_lot::Mutex;
use rand::Rng;
use tokio::io::AsyncReadExt;
use tokio::signal;
use tokio::sync::mpsc::WeakSender;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const TTL: Duration = Duration::from_secs(1);

struct OpenFile {
    cache: Bytes,
    offset: u64
}

#[derive(Copy, Clone)]
struct Range {
    offset: u64,
    size: u32
}

struct FilS3FS {
    s3client: Client,
    bucket: String,
    // handle -> (file size, cache)
    fil_mapping: parking_lot::RwLock<BTreeMap<u64, (u64, parking_lot::Mutex<OpenFile>)>>,
    aggregated_read_mapping: Arc<parking_lot::RwLock<AHashMap<String, WeakSender<(Range, tokio::sync::oneshot::Sender<anyhow::Result<RangePart>>)>>>>
}

#[derive(Clone)]
struct RoundRobin {
    inner: GaiResolver,
}

impl Service<Name> for RoundRobin {
    type Response = IntoIter<SocketAddr>;
    type Error = io::Error;
    type Future = MapOk<GaiFuture, fn(GaiAddrs) -> IntoIter<SocketAddr>>;

    fn poll_ready(&mut self, cx: &mut task::Context<'_>) -> Poll<anyhow::Result<(), io::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, name: Name) -> Self::Future {
        self.inner.call(name).map_ok(|v| {
            let mut list: Vec<SocketAddr> = v.collect();

            if list.len() > 1 {
                let i = rand::thread_rng().gen_range(0..list.len());
                list = vec![list[i]];
            }

            if !list.is_empty() {
                debug!("ip select {}", list[0]);
            }
            list.into_iter()
        })
    }
}

const DIC_ATTR: FileAttr = FileAttr {
    size: 0,
    blocks: 0,
    atime: SystemTime::UNIX_EPOCH,
    mtime: SystemTime::UNIX_EPOCH,
    ctime: SystemTime::UNIX_EPOCH,
    kind: FileType::Directory,
    perm: 0o644,
    nlink: 0,
    uid: 0,
    gid: 0,
    rdev: 0,
    blksize: 0,
};

impl FilS3FS {
    pub fn new(
        endpoint: &str,
        region: &str,
        ak: &str,
        sk: &str,
        bucket: &str,
    ) -> Self {
        let mut builder = SdkConfig::builder()
            .endpoint_url(endpoint)
            .region(Region::new(region.to_string()));

        builder = builder.credentials_provider(SharedCredentialsProvider::new(Credentials::new(
            ak,
            sk,
            None,
            None,
            "Static",
        )));

        let connector = HttpConnector::new_with_resolver(RoundRobin { inner: GaiResolver::new() });

        let client = HyperClientBuilder::new()
            .build(connector);

        builder.set_http_client(Some(client));

        let s3client = Client::new(&builder.build());

        FilS3FS {
            s3client,
            bucket: bucket.to_string(),
            fil_mapping: parking_lot::RwLock::new(BTreeMap::new()),
            aggregated_read_mapping: Arc::new(parking_lot::RwLock::new(AHashMap::new()))
        }
    }
}

fn is_dic(out: &HeadObjectOutput) -> bool {
    out.content_length.unwrap_or(0) == 0
}

// (start, end, total)
fn parse_range(range_str: &str) -> anyhow::Result<(u64, u64, u64)> {
    sscanf::scanf!(range_str, "bytes {}-{}/{}", u64, u64, u64)
        .map_err(|_| anyhow!("invalid range: {}", range_str))
}

fn range_to_string(range: (Option<u64>, Option<u64>)) -> anyhow::Result<String> {
    let str = match range {
        (Some(start), Some(end)) => format!("{}-{}", start, end),
        (Some(start), None) => format!("{}-", start),
        (None, Some(end)) => format!("-{}", end),
        _ => return Err(anyhow!("range params is invalid"))
    };
    Ok(str)
}

#[derive(Clone, Eq, PartialEq)]
pub struct RangePart {
    pub data: Bytes,
    // offset, len
    pub range: (u64, u64),
}

fn strip_out_boundary(content_type: &str) -> Option<&str> {
    sscanf::scanf!(
        content_type,
        "multipart/byteranges; boundary={}",
        str
    ).map_or_else(|_| sscanf::scanf!(
        content_type,
        "multipart/byteranges;boundary={}",
        str
    ), |v| Ok(v))
   .ok()
}

/// # Arguments
/// * `ranges` - (start position, length)
async fn read_multi_ranges(
    s3client: &Client,
    bucket: &str,
    key: &str,
    ranges: &[(u64, u64)],
) -> anyhow::Result<Vec<RangePart>> {
    let ranges: Vec<(Option<u64>, Option<u64>)> = ranges
        .iter()
        .map(|(start, len)| (Some(*start), Some(*start + len - 1)))
        .collect();

    let mut value = String::from("bytes=");
    value.push_str(&range_to_string(ranges[0])?);

    for i in 1..ranges.len() {
        value.push_str(", ");
        value.push_str(&range_to_string(ranges[i])?);
    }

    let resp= s3client.get_object()
        .bucket(bucket)
        .key(key)
        .range(value)
        .send()
        .await?;

    let content_type = resp.content_type.ok_or_else(|| anyhow!("Content-Type can't be empty"))?;

    let parts = match strip_out_boundary(&content_type) {
        None => {
            let (start, len) = match resp.content_range {
                None => {
                    let len = resp.content_length.ok_or_else(|| anyhow!("Content-Length can't be empty"))?;
                    (0, len as u64)
                }
                Some(range) => {
                    let (start, end, _) = parse_range(&range)?;
                    (start, end - start + 1)
                }
            };

            let mut buff = Vec::with_capacity(len as usize);
            resp.body.into_async_read().read_to_end(&mut buff).await?;

            let part = RangePart {
                data: Bytes::from(buff),
                range: (start, len),
            };
            vec![part]
        }
        Some(boundary) => {
            let cl = resp.content_length.unwrap_or(0);

            let mut buff = Vec::with_capacity(cl as usize);
            resp.body.into_async_read().read_to_end(&mut buff).await?;
            let mut parts = multipart::server::Multipart::with_body(buff.as_slice(), boundary);

            let mut list = Vec::with_capacity(ranges.len());

            while let Some(mut part) = parts.read_entry()? {
                let range = part.headers.content_range.ok_or_else(|| {
                   anyhow!("Content-Range can't be empty")
                })?;

                let (start, end, _): (u64, u64, u64) = parse_range(&range)?;
                let len = end - start + 1;

                let mut buff = Vec::with_capacity(len as usize);
                part.data.read_to_end(&mut buff)?;

                let part = RangePart {
                    data: Bytes::from(buff),
                    range: (start, len),
                };
                list.push(part);
            }
            list
        }
    };

    if parts.len() == ranges.len() {
        return Ok(parts)
    }

    // overlapping
    let v = parts.into_iter()
        .map(|part| Interval {
            start: part.range.0,
            stop: part.range.0 + part.range.1,
            val: part
        })
        .collect();

    let lapper = Lapper::new(v);
    let mut output = Vec::with_capacity(ranges.len());

    for (start, end) in ranges {
        let start = start.unwrap();
        let end = end.unwrap();
        let len = end - start + 1;

        let mut iv_opt = None;
        let mut it = lapper.find(start, start + 1);

        while let Some(iv) = it.next() {
            if start >= iv.start && end < iv.stop {
                iv_opt = Some(iv);
                break
            }
        };

        let iv = match iv_opt {
            None => return Err(anyhow!("Parse ranges failed")),
            Some(v) => v,
        };

        let data = Bytes::copy_from_slice(&iv.val.data[start as usize - iv.start as usize..start as usize - iv.start as usize + len as usize]);

        let part = RangePart {
            data,
            range: (start, len)
        };
        output.push(part)
    }

    Ok(output)
}

impl PathFilesystem for FilS3FS {
    type DirEntryStream<'a>
    = Empty<Result<DirectoryEntry>>
    where
        Self: 'a;

    async fn init(&self, _req: Request) -> Result<ReplyInit> {
        Ok(ReplyInit {
            max_write: NonZeroU32::new(1024 * 1024 * 64).unwrap(),
        })
    }

    async fn destroy(&self, _req: Request) {}

    async fn lookup(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<ReplyEntry> {
        let client = &self.s3client;
        let bucket = self.bucket.as_str();
        let key = Path::new(parent).join(name);
        let key = key.to_string_lossy();
        let key = key.trim_start_matches('/');

        let res = client.head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await;

        let out = match res {
            Ok(v) => v,
            Err(e) => {
                // error!("lookup head object failed; parent: {:?}, name: {:?}, key: {}, error: {}", parent, name, key, e);
                let attr = FileAttr {
                    size: 0,
                    blocks: 0,
                    atime: SystemTime::UNIX_EPOCH,
                    mtime: SystemTime::UNIX_EPOCH,
                    ctime: SystemTime::UNIX_EPOCH,
                    kind: FileType::Directory,
                    perm: 0o644,
                    nlink: 0,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    blksize: 0,
                };

                return Ok(ReplyEntry {
                    ttl: Duration::from_secs(1),
                    attr
                });
            }
        };

        let attr = FileAttr {
            size: out.content_length.unwrap_or(0) as u64,
            blocks: 0,
            atime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            ctime: SystemTime::UNIX_EPOCH,
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 0,
        };
        
        Ok(ReplyEntry {
            ttl: Duration::from_secs(1),
            attr
        })
    }

    async fn getattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: Option<u64>,
        _flags: u32,
    ) -> Result<ReplyAttr> {
        if path == Some(OsStr::new("/")) {
            let attr = FileAttr {
                size: 0,
                blocks: 0,
                atime: SystemTime::UNIX_EPOCH,
                mtime: SystemTime::UNIX_EPOCH,
                ctime: SystemTime::UNIX_EPOCH,
                kind: FileType::Directory,
                perm: 0o644,
                nlink: 0,
                uid: 0,
                gid: 0,
                rdev: 0,
                blksize: 0,
            };

            return Ok(ReplyAttr {
                ttl: Duration::from_secs(1),
                attr
            });
        }
        
        let client = &self.s3client;
        let bucket = self.bucket.as_str();
        let key = path.ok_or_else(Errno::new_not_exist)?.to_string_lossy();
        let key = key.trim_start_matches('/');

        let res = client.head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await;

        let out = match res {
            Ok(v) => v,
            Err(e) => {
                let attr = FileAttr {
                    size: 0,
                    blocks: 0,
                    atime: SystemTime::UNIX_EPOCH,
                    mtime: SystemTime::UNIX_EPOCH,
                    ctime: SystemTime::UNIX_EPOCH,
                    kind: FileType::Directory,
                    perm: 0o644,
                    nlink: 0,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    blksize: 0,
                };

                return Ok(ReplyAttr {
                    ttl: Duration::from_secs(1),
                    attr
                });
            }
        };

        let kind = if is_dic(&out) {
            FileType::Directory
        } else {
            FileType::RegularFile
        };

        let attr = FileAttr {
            size: out.content_length.unwrap_or(0) as u64,
            blocks: 0,
            atime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            ctime: SystemTime::UNIX_EPOCH,
            kind,
            perm: 0o644,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 0,
        };

        Ok(ReplyAttr {
            ttl: Duration::from_secs(1),
            attr
        })
    }

    async fn open(&self, _req: Request, path: &OsStr, flags: u32) -> Result<ReplyOpen> {
        let client = &self.s3client;
        let bucket = self.bucket.as_str();
        let openfils = &self.fil_mapping;
        
        let key = path.to_string_lossy();
        let key = key.trim_start_matches('/');

        let res = client.head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await;

        let out = match res {
            Ok(v) => v,
            Err(e) => {
                error!("open head object failed: {}", e);
                return Err(Errno::new_not_exist());
            }
        };
        
        if is_dic(&out) {
            return Err(Errno::new_is_dir());
        }
        
        let fh = rand::random();

        {
            let mut guard = openfils.write();
            let openfils = guard.deref_mut();

            openfils.insert(fh, (out.content_length.unwrap() as u64, parking_lot::Mutex::new(OpenFile {
                offset: 0,
                cache: Bytes::new()
            })));
        }
        
        Ok(ReplyOpen {fh, flags})
    }

    async fn read(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<ReplyData> {
        let client = &self.s3client;
        let bucket = self.bucket.as_str();
        let openfils = &self.fil_mapping;
        let aggregated_mapping = &self.aggregated_read_mapping;
        
        let key = path.ok_or_else(Errno::new_not_exist)?.to_string_lossy();
        let key = key.trim_start_matches('/');

        let file_len;
        let buf_offset = {
            let guard = openfils.read();
            let (len, p) = guard.get(&fh).ok_or_else(Errno::new_not_exist)?;

            if offset >= *len {
                return Ok(ReplyData::from(Bytes::new()));
            }

            file_len = *len;

            let mut guard = p.lock();
            let op = guard.deref_mut();

            if offset == op.offset && op.cache.remaining() >= size as usize {
                let read_len = std::cmp::min(op.cache.remaining(), size as usize);
                let ret = op.cache.split_to(read_len);
                op.offset += read_len as u64;

                return Ok(ReplyData::from(ret));
            }

            op.offset
        };
        
        let read_len = if buf_offset == offset {
            // 64MB
            1024 * 1024 * 64
        } else {
            size
        };

        let read_len = std::cmp::min(read_len as u64, file_len - offset) as u32;

        let tx = {
            let mut need_yield = false;

            loop {
                if need_yield {
                    tokio::task::yield_now().await;
                }
                let guard = aggregated_mapping.read();

                match guard.get(key) {
                    None => {
                        drop(guard);
                        let mut guard = aggregated_mapping.write();

                        match guard.get(key) {
                            None => {
                                let (tx, rx) = tokio::sync::mpsc::channel(8192);
                                guard.insert(key.to_string(), tx.downgrade());
                                drop(guard);

                                tokio::spawn({
                                    let tx = tx.clone();

                                    async move {
                                        tokio::time::sleep(Duration::from_secs(3)).await;

                                        while tx.strong_count() > 1 {
                                            tokio::time::sleep(Duration::from_secs(3)).await;
                                        }
                                        drop(tx);
                                    }
                                });

                                tokio::spawn({
                                    let s3client = client.clone();
                                    let bucket = bucket.to_string();
                                    let key = key.to_string();
                                    let aggregated_mapping = aggregated_mapping.clone();

                                    async move {
                                        let mut rx = rx;
                                        let mut parts = Vec::new();

                                        loop {
                                            let count = rx.recv_many(&mut parts, 8192).await;

                                            if count == 0 {
                                                break;
                                            }

                                            let recv = parts.drain(..parts.len()).collect::<Vec<_>>();

                                            let ranges = recv.iter()
                                                .map(|(part, _)| (part.offset, part.size as u64))
                                                .collect::<Vec<_>>();

                                            let res = read_multi_ranges(
                                                &s3client,
                                                &bucket,
                                                &key,
                                                &ranges,
                                            ).await;

                                            let list = match res {
                                                Ok(v) => v,
                                                Err(e) => {
                                                    for (_, callback) in recv {
                                                        let _ = callback.send(Err(anyhow!(e.to_string())));
                                                    }
                                                    continue;
                                                }
                                            };

                                            for ((_, callback), p) in recv.into_iter().zip(list) {
                                                let _ = callback.send(Ok(p));
                                            }
                                        }

                                        aggregated_mapping.write().remove(&key);
                                    }
                                });
                                break tx;
                            }
                            Some(tx) => {
                                if let Some(tx) = tx.upgrade() {
                                    break tx;
                                } else {
                                    need_yield = true;
                                }
                            }
                        }
                    }
                    Some(tx) => {
                        if let Some(tx) = tx.upgrade() {
                            break tx;
                        } else {
                            need_yield = true;
                        }
                    }
                }
            }
        };
        let range = Range {
            offset,
            size: read_len,
        };

        let (oneshot_tx, oneshot_rx) = tokio::sync::oneshot::channel();
        tx.send((range, oneshot_tx)).await.unwrap();

        let range_part = match oneshot_rx.await.unwrap() {
            Ok(v) => v,
            Err(e) => {
                error!("read failed: {}", e);
                return Err(Errno::new_not_exist());
            }
        };

        let mut data = range_part.data;
        let read_len = std::cmp::min(size as usize, data.len());
        let ret = data.split_to(read_len);
        let buf;
        {
            let guard = openfils.read();
            let (_, p) = guard.get(&fh).ok_or_else(Errno::new_not_exist)?;

            let mut guard = p.lock();
            let op = guard.deref_mut();

            buf = std::mem::replace(&mut op.cache, data);
            op.offset = offset + read_len as u64;
        }

        drop(buf);
        Ok(ReplyData { data: ret })
    }

    async fn release(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> Result<()> {
        self.fil_mapping.write().remove(&fh);
        Ok(())
    }

    async fn access(&self, _req: Request, _path: &OsStr, _mask: u32) -> Result<()> {
        Ok(())
    }

    type DirEntryPlusStream<'a>
    = Iter<IntoIter<Result<DirectoryEntryPlus>>>
    where
        Self: 'a;

    async fn readdirplus<'a>(
        &'a self,
        _req: Request,
        parent: &'a OsStr,
        _fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> Result<ReplyDirectoryPlus<Self::DirEntryPlusStream<'a>>> {

        let client = &self.s3client;
        let bucket = self.bucket.as_str();

        let parent_key = parent.to_string_lossy();
        let parent_key = parent_key.trim_start_matches('/');

        let dic = FileAttr {
            size: 0,
            blocks: 0,
            atime: SystemTime::UNIX_EPOCH,
            mtime: SystemTime::UNIX_EPOCH,
            ctime: SystemTime::UNIX_EPOCH,
            kind: FileType::Directory,
            perm: 0o644,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            blksize: 0,
        };

        let pre_children = vec![
            Ok(DirectoryEntryPlus {
                kind: FileType::Directory,
                name: OsString::from("."),
                offset: 1,
                attr: dic,
                entry_ttl: TTL,
                attr_ttl: TTL,
            }),
            Ok(DirectoryEntryPlus {
                kind: FileType::Directory,
                name: OsString::from(".."),
                offset: 2,
                attr: dic,
                entry_ttl: TTL,
                attr_ttl: TTL,
            }),
        ];

        let res = client.list_objects_v2()
            .bucket(bucket)
            .max_keys(i32::MAX)
            .prefix(parent_key)
            .send()
            .await;

        let output = match res {
            Ok(v) => v,
            Err(e) => {
                error!("readdirplus list objects failed: {}", e);
                return Err(Errno::new_not_exist());
            }
        };
        
        let objs = output.contents.unwrap_or_default();
        let mut dics = AHashSet::new();

        for obj in &objs {
            let key = Path::new(obj.key.as_ref().unwrap());
            let strip_key = key.strip_prefix(parent_key).unwrap();
            let mut iter = strip_key.iter();
        
            if let Some(name) = iter.next() {
                dics.insert(name.to_string_lossy().to_string());
            }
        }
        
        let obj_mapping = objs.into_iter()
            .map(|obj| (obj.key().unwrap().to_string(), obj))
            .collect::<AHashMap<_, _>>();
        
        let it = dics.into_iter()
            .enumerate()
            .map(|(offset, key)| {
                let (kind, size) = match obj_mapping.get(Path::new(parent).join(&key).to_string_lossy().as_ref()) {
                    None => {
                        (FileType::Directory, 0)
                    }
                    Some(obj) => {
                        let kind = if obj.size.unwrap_or_default() == 0 {
                            FileType::Directory
                        } else {
                            FileType::RegularFile
                        };
                        (kind, obj.size.unwrap_or_default())
                    }
                };

                let attr = FileAttr {
                    size: size as u64,
                    blocks: 0,
                    atime: SystemTime::UNIX_EPOCH,
                    mtime: SystemTime::UNIX_EPOCH,
                    ctime: SystemTime::UNIX_EPOCH,
                    kind,
                    perm: 0o644,
                    nlink: 0,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    blksize: 0,
                };

                let entry = DirectoryEntryPlus {
                    kind,
                    name: OsString::from(key),
                    offset: offset as i64 + 3,
                    attr,
                    entry_ttl: TTL,
                    attr_ttl: TTL,
                };
                Ok(entry)
            });
     
        let out= pre_children.into_iter()
            .chain(it)
            .skip(offset as usize)
            .collect::<Vec<_>>();
        Ok(ReplyDirectoryPlus {
            entries: stream::iter(out),
        })
    }

    async fn lseek(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        fh: u64,
        offset: u64,
        whence: u32,
    ) -> Result<ReplyLSeek> {
        let openfils = &self.fil_mapping;

        let whence = whence as i32;
        let file_size;
        let mut curr_offset;

        {
            match openfils.read().get(&fh) {
                None => return Err(Errno::new_not_exist()),
                Some(((size, p))) => {
                    file_size = *size;
                    curr_offset = p.lock().offset;
                }
            }
        }

        let new_offset = match whence {
            libc::SEEK_SET => offset,
            libc::SEEK_CUR => {
                std::cmp::min(curr_offset + offset, file_size)
            },
            libc::SEEK_END => {
               std::cmp::max(curr_offset - offset, file_size)
            }
            _ => return Err(libc::EINVAL.into())
        };

        Ok(ReplyLSeek { offset: new_offset })
    }

    async fn mkdir(
        &self,
        _req: Request,
        _parent: &OsStr,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
    ) -> Result<ReplyEntry> {
        Ok(ReplyEntry {
            ttl: TTL,
            attr: DIC_ATTR
        })
    }

    async fn unlink(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<()> {
        let client = &self.s3client;
        let bucket = self.bucket.as_str();

        let key = Path::new(parent).join(name);
        let key = key.to_string_lossy();
        let key = key.trim_start_matches('/');

        let res = client.delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await;

        if let Err(e) = res {
            error!("unlink delete object error: {}", e);
            return Err(Errno::new_not_exist());
        }

        Ok(())
    }

    async fn rmdir(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<()> {
        let client = &self.s3client;
        let bucket = self.bucket.as_str();

        let key = Path::new(parent).join(name);
        let key = key.to_string_lossy();
        let key = key.trim_start_matches('/');

        let res = client.list_objects_v2()
            .bucket(bucket)
            .max_keys(i32::MAX)
            .prefix(key)
            .send()
            .await;

        let output = match res {
            Ok(v) => v,
            Err(e) => {
                error!("rmdir list objects failed: {}", e);
                return Err(Errno::new_not_exist());
            }
        };

        let objs = output.contents.unwrap_or_default();

        let dels= objs.into_iter()
            .filter(|obj| obj.size.unwrap_or_default() > 0)
            .map(|obj| {
                ObjectIdentifier::builder()
                    .key(obj.key.unwrap())
                    .build()
                    .unwrap()
            })
            .collect::<Vec<_>>();

        let del = Delete::builder()
            .set_objects(Some(dels))
            .quiet(true)
            .build()
            .unwrap();

        let res = client.delete_objects()
            .bucket(bucket)
            .delete(del)
            .send()
            .await;

        if let Err(e) = res {
            error!("rmdir delete objects error: {}", e);
        }
        Ok(())
    }

    async fn write(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: u64,
        offset: u64,
        data: &[u8],
        _write_flags: u32,
        _flags: u32,
    ) -> Result<ReplyWrite> {
        todo!()
    }
}

fn log_init() -> anyhow::Result<()>{
    let pattern = if cfg!(debug_assertions) {
        "[{d(%Y-%m-%d %H:%M:%S)}] {h({l})} {f}:{L} - {m}{n}"
    } else {
        "[{d(%Y-%m-%d %H:%M:%S)}] {h({l})} {t} - {m}{n}"
    };

    let stdout = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new(pattern)))
        .build();

    let config = log4rs::Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(
            Root::builder()
                .appender("stdout")
                .build(LevelFilter::from_str(
                    std::env::var("FILS3_FUSE").as_deref().unwrap_or("INFO"),
                )?),
        )?;

    log4rs::init_config(config)?;
    Ok(())
}

#[derive(Debug, Clone, Deserialize)]
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Parser)]
#[command(version)]
struct Args {
    #[arg(short, long)]
    mount_path: PathBuf,

    #[arg(short, long)]
    s3config_path: PathBuf,
}

async fn exec(args: Args) -> anyhow::Result<()> {
    let uid = unsafe { libc::getuid() };
    let gid = unsafe { libc::getgid() };

    let mut mount_options = MountOptions::default();
    mount_options.force_readdir_plus(true).uid(uid).gid(gid);

    let config_str = std::fs::read_to_string(&args.s3config_path)?;
    let s3config: S3Config = toml::from_str(&config_str)?;

    let fs = FilS3FS::new(
        &s3config.endpoint,
        &s3config.region,
        &s3config.access_key,
        &s3config.secret_key,
        &s3config.bucket
    );

    let mut mount_handle = Session::new(mount_options)
        .mount(fs, args.mount_path)
        .await?;

    let mut terminate = signal::unix::signal(signal::unix::SignalKind::terminate())?;
    let mut interrupt = signal::unix::signal(signal::unix::SignalKind::interrupt())?;

    let handle = &mut mount_handle;

    tokio::select! {
        _ = terminate.recv() => {
            mount_handle.unmount().await?;
        },
        _ = interrupt.recv() => {
            mount_handle.unmount().await?;
        },
        res = handle => {
            res?;
        },
    }
    Ok(())
}

fn main() -> ExitCode {
    let args: Args = Args::parse();
    let rt = tokio::runtime::Runtime::new().unwrap();
    log_init().unwrap();

    match rt.block_on(exec(args)) {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{:?}", e);
            ExitCode::FAILURE
        }
    }
}