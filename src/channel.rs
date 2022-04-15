use futures::future;
use glommio::channels::shared_channel::{
    self, ConnectedReceiver, ConnectedSender, SharedReceiver, SharedSender,
};

#[derive(Debug)]
pub struct ConnectedBiChannel<T: 'static + Send + Sized, S: 'static + Send + Sized> {
    sender: ConnectedSender<T>,
    receiver: ConnectedReceiver<S>,
}

impl<T: 'static + Send + Sized, S: 'static + Send + Sized> ConnectedBiChannel<T, S> {
    pub async fn send(&self, msg: T) -> glommio::Result<(), T> {
        self.sender.send(msg).await
    }

    pub async fn recv(&self) -> Option<S> {
        self.receiver.recv().await
    }
}

#[derive(Debug)]
pub struct SharedBiChannel<T: Send + Sized, S: Send + Sized> {
    sender: SharedSender<T>,
    receiver: SharedReceiver<S>,
}

impl<T: 'static + Send + Sized, S: 'static + Send + Sized> SharedBiChannel<T, S> {
    pub async fn connect(self) -> ConnectedBiChannel<T, S> {
        let (sender, receiver) = future::join(self.sender.connect(), self.receiver.connect()).await;

        ConnectedBiChannel { sender, receiver }
    }

    pub fn split(self) -> (SharedSender<T>, SharedReceiver<S>) {
        (self.sender, self.receiver)
    }
}

pub fn create_bi_channel<T: Send + Sized, S: Send + Sized>(
    cap: usize,
) -> (SharedBiChannel<T, S>, SharedBiChannel<S, T>) {
    let (sender_a, receiver_a) = shared_channel::new_bounded(cap);
    let (sender_b, receiver_b) = shared_channel::new_bounded(cap);

    (
        SharedBiChannel {
            sender: sender_a,
            receiver: receiver_b,
        },
        SharedBiChannel {
            sender: sender_b,
            receiver: receiver_a,
        },
    )
}

#[cfg(test)]
mod tests {
    use glommio::LocalExecutorBuilder;

    use super::*;

    #[test]
    fn test_two_threads() {
        let (a, b) = create_bi_channel(10);

        let handle_a = LocalExecutorBuilder::default()
            .spawn(|| async move {
                let a = a.connect().await;
                a.send("hello").await.unwrap();
                let msg = a.recv().await.unwrap();
                assert_eq!(msg, b"world");
            })
            .unwrap();

        let handle_b = LocalExecutorBuilder::default()
            .spawn(|| async move {
                let b = b.connect().await;
                let msg = b.recv().await.unwrap();
                assert_eq!(msg, "hello");
                b.send(b"world").await.unwrap();
            })
            .unwrap();

        handle_a.join().unwrap();
        handle_b.join().unwrap();
    }

    #[test]
    fn test_one_thread() {
        let (a, b) = create_bi_channel(10);

        let handle = LocalExecutorBuilder::default()
            .spawn(|| async move {
                let (a, b) = future::join(a.connect(), b.connect()).await;

                a.send("hello").await.unwrap();
                let msg = b.recv().await.unwrap();
                assert_eq!(msg, "hello");

                b.send("world").await.unwrap();
                let msg = a.recv().await.unwrap();
                assert_eq!(msg, "world");
            })
            .unwrap();

        handle.join().unwrap();
    }
}
