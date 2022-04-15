use std::{cell::RefCell, ops::Deref, path::Path, rc::Rc};

use ahash::AHashMap;
use eyre::{bail, ensure, Result};
use futures::StreamExt;
use glommio::{
    channels::{
        local_channel::{self, LocalSender},
        shared_channel::ConnectedSender,
    },
    io::{Directory, ReadResult},
    spawn_local, CpuSet, ExecutorJoinHandle, LocalExecutorBuilder,
};
use tracing::{trace, warn};

use crate::{
    channel::{self, ConnectedBiChannel},
    lsm::{KeyValueStorage, LogStorage},
    partition::{choose_bucket, Bucket},
    sth::{Config as SthConfig, StoreTheHash},
    vlog::{VLog, VLogConfig},
};

const BUCKET_SIZE: u8 = 24;

/// A shard that runs on a single thread.
#[derive(Debug)]
pub struct JasmineShard {
    id: u64,
    vlog: VLog,
    index: StoreTheHash<<VLog as LogStorage>::Offset, VLog, BUCKET_SIZE>,
    rpc_channel: ConnectedBiChannel<Response, Request>,
}

impl JasmineShard {
    async fn create<P: AsRef<Path>>(
        id: u64,
        path: P,
        rpc_channel: ConnectedBiChannel<Response, Request>,
    ) -> Result<Self> {
        let path = path.as_ref();
        ensure!(path.is_dir(), "path must be a directory");
        let vlog_dir = Directory::create(path.join(format!("vlog-{id}")))
            .await
            .map_err(|e| eyre::eyre!("failed to create vlog dir: {}", e))?;

        let vlog_file = vlog_dir.path().unwrap().join("values.vlog");
        let vlog = VLog::create(VLogConfig::new(vlog_file)).await?;
        let index = StoreTheHash::create(SthConfig::new(vlog.clone())).await?;
        vlog_dir.close().await.map_err(|e| eyre::eyre!("{}", e))?;

        Ok(Self {
            id,
            vlog,
            index,
            rpc_channel,
        })
    }

    async fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> Result<()> {
        trace!("put: {}", key.as_ref().len());
        let offset = self.vlog.put(key.as_ref(), value).await?;
        self.index.put(key, offset).await?;
        Ok(())
    }

    async fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Slice>> {
        trace!("get {}", key.as_ref().len());
        if let Some(offset) = self.index.get(key.as_ref()).await? {
            if let Some((stored_key, value)) = self.vlog.get(&offset).await? {
                ensure!(&*stored_key == key.as_ref(), "key missmatch");
                return Ok(Some(Slice(value)));
            } else {
                bail!("missing value in value log for key: {:?}", key.as_ref());
            }
        }

        Ok(None)
    }

    async fn flush(&self) -> Result<()> {
        // self.index.flush().await?;
        self.vlog.flush().await?;
        Ok(())
    }

    async fn run(self) {
        let cur_shard = self.id;
        let id = loop {
            if let Some(req) = self.rpc_channel.recv().await {
                trace!("handling request {} at {}", req.id, cur_shard);
                let id = req.id;
                match req.value {
                    RequestValue::Put { key, value } => {
                        let res = self.put(key, value).await;
                        self.rpc_channel
                            .send(Response {
                                id,
                                value: ResponseValue::Put(res),
                            })
                            .await
                            .unwrap();
                    }
                    RequestValue::Get { key } => {
                        let res = self.get(key).await;
                        self.rpc_channel
                            .send(Response {
                                id,
                                value: ResponseValue::Get(res.map(|v| v.map(|v| v.to_vec()))),
                            })
                            .await
                            .unwrap();
                    }
                    RequestValue::Flush => {
                        let res = self.flush().await;
                        self.rpc_channel
                            .send(Response {
                                id,
                                value: ResponseValue::Flush(res),
                            })
                            .await
                            .unwrap();
                    }

                    RequestValue::Close => {
                        // shutting down, so break
                        break id;
                    }
                }
            } else {
                panic!("sad panda");
            }
        };

        let ret1 = self.index.close().await;
        let ret2 = self.vlog.close().await;
        let ret = ret1.and_then(|_| ret2);

        self.rpc_channel
            .send(Response {
                id,
                value: ResponseValue::Close(ret),
            })
            .await
            .unwrap();
    }
}

/// Opaque result that gets returned for stored values, to avoid copying data.
#[derive(Debug, Clone)]
pub struct Slice(ReadResult);

impl Deref for Slice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

pub struct Jasmine {
    executors: Vec<ExecutorJoinHandle<()>>,
    buckets: Vec<Bucket>,
    rpc_channels: Vec<ConnectedSender<Request>>,
    next_req_id: RefCell<usize>,
    outstanding_requests: Rc<RefCell<AHashMap<usize, local_channel::LocalSender<Response>>>>,
}

impl Jasmine {
    pub async fn create<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        ensure!(path.is_dir(), "path must be a directory");

        // TODO: use num_cpus/configurable
        let cpu_set = CpuSet::online().map_err(|e| eyre::eyre!("unable to detect cpus {:?}", e))?;

        let num_shards = cpu_set.len();

        // TODO: more intelligent placement
        // TODO: better default for channel sizes

        let mut executors = Vec::with_capacity(num_shards - 1);
        let buckets = (0..num_shards)
            .map(|id| Bucket::new(id as u64, 100., id as u64 * 100))
            .collect();

        let mut rpc_channel_senders = Vec::with_capacity(num_shards);
        let mut rpc_channel_receivers = Vec::with_capacity(num_shards);

        for id in 0..num_shards {
            let path = path.clone();
            let (rpc_sender, rpc_receiver) = channel::create_bi_channel(1024);
            let executor = LocalExecutorBuilder::default()
                .name(&format!("jasmine-worker-{id}"))
                .spawn(move || async move {
                    let rpc_receiver = rpc_receiver.connect().await;
                    let shard = JasmineShard::create(id as u64, path, rpc_receiver)
                        .await
                        .unwrap();
                    trace!("created shard {}", id);
                    shard.run().await;
                })
                .map_err(|err| eyre::eyre!("failed to spawn: {:?}", err))?;
            executors.push(executor);

            let (sender, recv) = rpc_sender.split();
            rpc_channel_senders.push(sender);
            rpc_channel_receivers.push(recv);
        }
        trace!("setup done");

        let outstanding_requests = Rc::new(RefCell::new(AHashMap::new()));
        let or = outstanding_requests.clone();

        spawn_local(async move {
            let mut connected_receivers = Vec::with_capacity(num_shards);
            for channel in rpc_channel_receivers.into_iter() {
                connected_receivers.push(channel.connect().await);
            }

            let mut message_stream = futures::stream::select_all(connected_receivers);
            while let Some(resp) = message_stream.next().await {
                let response_channel: Option<LocalSender<Response>> =
                    or.borrow_mut().remove(&resp.id);
                match response_channel {
                    Some(chan) => chan.send(resp).await.unwrap(),
                    None => warn!(
                        "got response for missing request {}: {:?}",
                        resp.id, resp.value
                    ),
                }
            }
        })
        .detach();

        let mut rpc_channels = Vec::with_capacity(num_shards);
        for chan in rpc_channel_senders.into_iter() {
            rpc_channels.push(chan.connect().await);
        }

        Ok(Self {
            executors,
            buckets,
            rpc_channels,
            next_req_id: RefCell::new(0),
            outstanding_requests,
        })
    }

    async fn send_to(&self, shard_id: u64, req: RequestValue) -> Result<ResponseValue> {
        ensure!(shard_id < self.rpc_channels.len() as u64, "invalid id");
        let req_id = *self.next_req_id.borrow();
        *self.next_req_id.borrow_mut() += 1;

        let (sender, receiver) = local_channel::new_bounded(1);
        self.outstanding_requests
            .borrow_mut()
            .insert(req_id, sender);

        // TODO: remove if the we have an error

        self.rpc_channels[shard_id as usize]
            .send(Request {
                id: req_id,
                value: req,
            })
            .await
            .map_err(|err| eyre::eyre!("failed to send to {}: {:?}", shard_id, err))?;

        let result = receiver
            .recv()
            .await
            .ok_or_else(|| eyre::eyre!("disconnected"))?;

        ensure!(
            result.id == req_id,
            "invalid response id: {} != {}",
            result.id,
            req_id
        );
        Ok(result.value)
    }

    pub async fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&self, key: K, value: V) -> Result<()> {
        let shard_id = self.find_shard(key.as_ref());

        trace!("send_to {}", shard_id);
        let response = self
            .send_to(
                shard_id,
                RequestValue::Put {
                    key: key.as_ref().to_vec(),
                    value: value.as_ref().to_vec(),
                },
            )
            .await?;

        trace!("response_from {}", shard_id);
        if let ResponseValue::Put(res) = response {
            res
        } else {
            Err(eyre::eyre!("invalid response: {:?}", response))
        }
    }

    pub async fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>> {
        let shard_id = self.find_shard(key.as_ref());

        let response = self
            .send_to(
                shard_id,
                RequestValue::Get {
                    key: key.as_ref().to_vec(),
                },
            )
            .await?;

        if let ResponseValue::Get(res) = response {
            res
        } else {
            Err(eyre::eyre!("invalid response: {:?}", response))
        }
    }

    pub async fn flush(&self) -> Result<()> {
        let mut list = Vec::with_capacity(self.buckets.len());

        for bucket in &self.buckets {
            list.push(self.send_to(bucket.id, RequestValue::Flush));
        }
        futures::future::join_all(list)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        Ok(())
    }

    pub async fn close(self) -> Result<()> {
        for bucket in &self.buckets {
            trace!("closing {}", bucket.id);
            let response = self.send_to(bucket.id, RequestValue::Close).await?;
            if let ResponseValue::Close(res) = response {
                res?;
            } else {
                bail!("invalid response: {:?}", response);
            }
        }

        for ex in self.executors.into_iter() {
            trace!("shutting down executor {:?}", ex.thread().id());
            ex.join().map_err(|e| eyre::eyre!("{:?}", e))?;
        }

        Ok(())
    }

    pub fn find_shard<K: AsRef<[u8]>>(&self, key: K) -> u64 {
        let shard = choose_bucket(&self.buckets, key.as_ref())
            .map(|b| b.id)
            .unwrap_or(0);
        debug_assert!(shard < self.buckets.len() as u64);
        shard
    }
}

#[derive(Debug)]
struct Request {
    id: usize,
    value: RequestValue,
}

#[derive(Debug)]
enum RequestValue {
    // TODO: avoid allocation
    Put { key: Vec<u8>, value: Vec<u8> },
    Get { key: Vec<u8> },
    Flush,
    Close,
}

#[derive(Debug)]
struct Response {
    id: usize,
    value: ResponseValue,
}

#[derive(Debug)]
enum ResponseValue {
    Put(Result<()>),
    Get(Result<Option<Vec<u8>>>),
    Flush(Result<()>),
    Close(Result<()>),
}

#[cfg(test)]
mod tests {
    use tracing::info;

    use super::*;

    #[test]
    fn test_basics() {
        tracing_subscriber::fmt()
            .pretty()
            .with_thread_names(true)
            .with_max_level(tracing::Level::TRACE)
            .init();

        use glommio::LocalExecutorBuilder;
        LocalExecutorBuilder::default()
            .name("main")
            .spawn(|| async move {
                let dir = tempfile::tempdir().unwrap();
                let jasmine = Jasmine::create(dir.path()).await?;

                const NUM_ENTRIES: u64 = 100;
                for i in 0..NUM_ENTRIES {
                    info!("hash entry {}", i);
                    let value = vec![i as u8; 32];
                    let key = blake3::hash(&value);
                    info!("put entry {}", i);
                    jasmine.put(key.as_bytes(), &value).await?;
                    info!("get entry {}", i);
                    let res = jasmine.get(key.as_bytes()).await?.unwrap();
                    assert_eq!(res, value);
                }

                jasmine.flush().await?;

                // for i in 0..NUM_ENTRIES {
                //     let value = vec![i as u8; 32];
                //     let key = blake3::hash(&value);
                //     let res = jasmine.get(key.as_bytes()).await?.unwrap();
                //     assert_eq!(res, value);
                // }

                jasmine.close().await?;

                Ok::<_, eyre::Error>(())
            })
            .unwrap()
            .join()
            .unwrap()
            .unwrap();
    }
}
