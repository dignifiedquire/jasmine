//! vLog, value log.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use eyre::Result;
use futures_lite::AsyncWriteExt;
use glommio::io::{DmaFile, DmaStreamWriter, DmaStreamWriterBuilder, OpenOptions, ReadResult};
use zerocopy::{AsBytes, FromBytes, LayoutVerified, LittleEndian, Unaligned, U16, U32};

use crate::lsm::LogStorage;

const VLOG_VERSION: u16 = 0;

#[derive(Debug)]
pub struct VLog {
    #[allow(dead_code)]
    header: VLogHeader,
    write_head: u64,
    writer: DmaStreamWriter,
    reader: DmaFile,
}

#[derive(Debug, Copy, Clone, FromBytes, AsBytes, Unaligned)]
#[repr(C)]
struct VLogHeader {
    version: U16<LittleEndian>,
}

impl Default for VLogHeader {
    fn default() -> Self {
        VLogHeader {
            version: VLOG_VERSION.into(),
        }
    }
}

impl VLogHeader {
    const fn size() -> usize {
        std::mem::size_of::<Self>()
    }
}

#[derive(Debug, Clone)]
pub struct VLogConfig {
    pub path: PathBuf,
}

impl VLogConfig {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        VLogConfig {
            path: path.as_ref().into(),
        }
    }
}

#[async_trait(?Send)]
impl LogStorage for VLog {
    type Config = VLogConfig;
    type Key = ReadResult;
    type Value = ReadResult;
    type Offset = u64;

    /// Creates a new, empty `VLog` at the provided `path`.
    async fn create(config: Self::Config) -> Result<Self> {
        let writer_file = DmaFile::create(&config.path)
            .await
            .map_err(|err| eyre::eyre!("failed create file: {:?}", err))?;
        let mut writer = DmaStreamWriterBuilder::new(writer_file).build();
        let reader = OpenOptions::new()
            .read(true)
            .dma_open(&config.path)
            .await
            .map_err(|err| eyre::eyre!("failed top open file: {:?}", err))?;
        let header = VLogHeader::default();
        writer.write_all(header.as_bytes()).await?;
        let write_head = VLogHeader::size() as u64;

        Ok(VLog {
            header,
            writer,
            reader,
            write_head,
        })
    }

    /// Open an existing `VLog` at the provided `path`.
    async fn open(config: Self::Config) -> Result<Self> {
        let reader = OpenOptions::new()
            .read(true)
            .dma_open(&config.path)
            .await
            .map_err(|err| eyre::eyre!("failed top open file: {:?}", err))?;

        let aligned_size = reader.align_up(VLogHeader::size() as u64);
        let header_bytes = reader
            .read_at_aligned(0, usize::try_from(aligned_size)?)
            .await
            .map_err(|err| eyre::eyre!("failed to read vLog header: {:?}", err))?;
        let (header, _) = LayoutVerified::<_, VLogHeader>::new_from_prefix(&header_bytes[..])
            .ok_or_else(|| eyre::eyre!("invalid VLog header"))?;

        // TODO: restore from a safer place
        let write_head = reader
            .file_size()
            .await
            .map_err(|err| eyre::eyre!("failed to read file size {:?}", err))?;

        let writer = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .dma_open(&config.path)
            .await
            .map_err(|err| eyre::eyre!("failed top open file: {:?}", err))?;
        let writer = DmaStreamWriterBuilder::new(writer).build();

        Ok(VLog {
            header: *header,
            reader,
            writer,
            write_head,
        })
    }

    /// Waits for all underlying data to be flushed and cleanly closes the underlyling
    /// file descriptors. Must be always called, as there is no `Drop` guard.
    async fn close(mut self) -> Result<()> {
        self.writer.close().await?;
        self.reader
            .close()
            .await
            .map_err(|err| eyre::eyre!("failed to close reader: {:?}", err))?;
        Ok(())
    }

    async fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &mut self,
        key: K,
        value: V,
    ) -> Result<Self::Offset> {
        let key = key.as_ref();
        let value = value.as_ref();

        let key_size: u16 = key.len().try_into()?;
        let value_size: u32 = value.len().try_into()?;
        let header = LogLineHeader::new(key_size, value_size);

        // write header
        self.writer.write_all(header.as_bytes()).await?;
        // write key & value
        self.writer.write_all(key).await?;
        self.writer.write_all(value).await?;

        let old_write_head = self.write_head;
        self.write_head += LogLineHeader::size() as u64 + key_size as u64 + value_size as u64;

        Ok(old_write_head)
    }

    async fn get(&mut self, offset: &Self::Offset) -> Result<Option<(Self::Key, Self::Value)>> {
        // read at least the header
        let header_size = LogLineHeader::size();
        let data = self.read_at_least(*offset, header_size).await?;

        // parse the header
        let (header, rest) = LayoutVerified::<_, LogLineHeader>::new_from_prefix(&data[..])
            .ok_or_else(|| eyre::eyre!("invalid log line header"))?;

        let (key, value) = if header.body_size() as usize <= rest.len() {
            // fast path, already read all we need

            let key = ReadResult::slice(&data, header_size, header.key_size() as usize)
                .expect("already validated");
            let value = ReadResult::slice(
                &data,
                header_size + header.key_size() as usize,
                header.value_size() as usize,
            )
            .expect("already validated");
            (key, value)
        } else if header.key_size() as usize <= rest.len() {
            // we have already read the key
            let key = ReadResult::slice(&data, header_size, header.key_size() as usize)
                .expect("already validated");

            let data = self
                .read_at_least(
                    *offset as u64 + header_size as u64 + header.key_size() as u64,
                    header.value_size() as usize,
                )
                .await?;
            let value = ReadResult::slice(&data, 0, header.value_size() as usize)
                .expect("already validated");
            (key, value)
        } else {
            // not enough for anything, read both key and value
            let data = self
                .read_at_least(
                    *offset as u64 + header_size as u64,
                    header.body_size() as usize,
                )
                .await?;
            let key =
                ReadResult::slice(&data, 0, header.key_size() as usize).expect("already validated");
            let value = ReadResult::slice(
                &data,
                header.key_size() as usize,
                header.value_size() as usize,
            )
            .expect("already validated");

            (key, value)
        };
        debug_assert_eq!(key.len(), header.key_size() as usize);
        debug_assert_eq!(value.len(), header.value_size() as usize);

        Ok(Some((key, value)))
    }

    async fn flush(&mut self) -> Result<()> {
        self.writer.flush().await?;
        Ok(())
    }
}

impl VLog {
    /// Reads at the given offset, at least `min_len`, will read more than that, returning the full result+
    /// from the read amplification through read alignment.
    async fn read_at_least<'a>(&mut self, offset: u64, min_len: usize) -> Result<ReadResult> {
        // align offset appropriately (downwards)
        let aligned_offset = self.reader.align_down(offset);
        let offset_diff = offset - aligned_offset;
        let read_len = offset_diff + min_len as u64;
        // align total read size appropriately (upwards)
        let aligned_read_len = self.reader.align_up(read_len);

        let buffer = self
            .reader
            .read_at_aligned(aligned_offset, usize::try_from(aligned_read_len)?)
            .await
            .map_err(|err| eyre::eyre!("read_at failed: {:?}", err))?;

        // remove alignment offset at the beginning
        let rest_len = buffer.len() - offset_diff as usize;
        let buffer = ReadResult::slice(&buffer, usize::try_from(offset_diff)?, rest_len)
            .expect("already validated");

        Ok(buffer)
    }
}

#[derive(Debug, FromBytes, AsBytes, Unaligned)]
#[repr(C)]
struct LogLineHeader {
    // this means keys are limited to 2**16, in practice want to limit to 4KiB probablay
    key_size: U16<LittleEndian>,
    // values are limited to 2**32 -1
    value_size: U32<LittleEndian>,
}

impl LogLineHeader {
    fn new(key_size: u16, value_size: u32) -> Self {
        LogLineHeader {
            key_size: key_size.into(),
            value_size: value_size.into(),
        }
    }

    #[inline]
    const fn size() -> usize {
        std::mem::size_of::<Self>()
    }

    fn key_size(&self) -> u16 {
        self.key_size.get()
    }

    fn value_size(&self) -> u32 {
        self.value_size.get()
    }

    #[inline]
    fn body_size(&self) -> u32 {
        self.key_size.get() as u32 + self.value_size.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_vlog_write_read_small_values() {
        use glommio::LocalExecutorBuilder;
        LocalExecutorBuilder::default()
            .spawn(|| async move {
                // your code here
                let file = tempfile::NamedTempFile::new().unwrap();

                let mut vlog = VLog::create(VLogConfig::new(file.path())).await?;

                let mut offsets = Vec::new();
                for i in 0u64..100 {
                    let offset = vlog
                        .put(i.to_le_bytes(), format!("hello world: {i}").as_bytes())
                        .await?;
                    offsets.push(offset);
                }

                // wait for all things to be actually written
                vlog.flush().await?;

                for (i, offset) in offsets.iter().enumerate() {
                    println!("reading {i}: at {offset}");
                    let (key, value) = vlog.get(offset).await?.unwrap();
                    assert_eq!(&key[..], &i.to_le_bytes()[..]);
                    assert_eq!(
                        std::str::from_utf8(&value).unwrap(),
                        format!("hello world: {i}")
                    );
                }

                Ok::<_, eyre::Error>(())
            })
            .unwrap()
            .join()
            .unwrap()
            .unwrap();
    }

    #[test]
    fn test_basic_vlog_write_read_large_values() {
        use glommio::LocalExecutorBuilder;
        LocalExecutorBuilder::default()
            .spawn(|| async move {
                // your code here
                let file = tempfile::NamedTempFile::new().unwrap();

                let mut vlog = VLog::create(VLogConfig::new(file.path())).await?;

                let mut offsets = Vec::new();
                for i in 0u64..100 {
                    let value = (0u64..500)
                        .flat_map(|k| (k * i).to_le_bytes())
                        .collect::<Vec<_>>();
                    let offset = vlog.put(i.to_le_bytes(), &value).await?;
                    offsets.push(offset);
                }

                // wait for all things to be actually written
                vlog.flush().await?;

                for (i, offset) in offsets.iter().enumerate() {
                    let i = i as u64;
                    println!("reading {i}: at {offset}");
                    let (key, value) = vlog.get(offset).await?.unwrap();
                    assert_eq!(&key[..], &i.to_le_bytes()[..]);
                    let original_value = (0u64..500)
                        .flat_map(|k| (k * i).to_le_bytes())
                        .collect::<Vec<_>>();

                    assert_eq!(&value[..], &original_value[..]);
                }

                Ok::<_, eyre::Error>(())
            })
            .unwrap()
            .join()
            .unwrap()
            .unwrap();
    }

    #[test]
    fn test_vlog_open() {
        use glommio::LocalExecutorBuilder;
        LocalExecutorBuilder::default()
            .spawn(|| async move {
                // your code here
                let file = tempfile::NamedTempFile::new().unwrap();

                let mut vlog = VLog::create(VLogConfig::new(file.path())).await?;

                let mut offsets = Vec::new();
                for i in 0u64..100 {
                    let offset = vlog
                        .put(i.to_le_bytes(), format!("hello world: {i}").as_bytes())
                        .await?;
                    offsets.push(offset);
                }

                vlog.close().await?;

                let mut vlog = VLog::open(VLogConfig::new(file.path())).await?;

                for (i, offset) in offsets.iter().enumerate() {
                    println!("reading {i}: at {offset}");
                    let (key, value) = vlog.get(offset).await?.unwrap();
                    assert_eq!(&key[..], &i.to_le_bytes()[..]);
                    assert_eq!(
                        std::str::from_utf8(&value).unwrap(),
                        format!("hello world: {i}")
                    );
                }

                Ok::<_, eyre::Error>(())
            })
            .unwrap()
            .join()
            .unwrap()
            .unwrap();
    }
}
