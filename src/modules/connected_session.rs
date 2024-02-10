use anyhow::Context;
use prost::Message;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{lookup_host, TcpStream, ToSocketAddrs},
};
use tracing::instrument;

#[allow(async_fn_in_trait)]

///
/// Trait for a session with a torrent server.
///
/// Used to abstract the internal sending and receiving of buffers.
/// See https://blog.rust-lang.org/2023/12/21/async-fn-rpit-in-traits.html
/// for why we need `trait_variant::make`
#[trait_variant::make(SocketWrapper: Send)]
pub trait LocalSocketWrapper: Send {
    async fn connect<A: ToSocketAddrs + Send>(addr: A) -> anyhow::Result<Self>
    where
        Self: Sized;
    async fn send(&mut self, buffer: &[u8]) -> anyhow::Result<()>;
    async fn recv(&mut self, buffer: &mut [u8]) -> anyhow::Result<Option<usize>>;
}

#[derive(Debug)]
pub struct TokioSocketWrapper {
    session: TcpStream,
}

impl TokioSocketWrapper {
    pub fn new(session: TcpStream) -> Self {
        Self { session }
    }
}

impl SocketWrapper for TokioSocketWrapper {
    async fn send(&mut self, buffer: &[u8]) -> anyhow::Result<()> {
        self.session
            .write_all(buffer)
            .await
            .context("failed to write to socket")
    }

    async fn recv(&mut self, buffer: &mut [u8]) -> anyhow::Result<Option<usize>> {
        let result = match self.session.read_exact(buffer).await {
            Ok(result) => result,
            Err(err) => {
                if err.kind() == std::io::ErrorKind::UnexpectedEof {
                    return Ok(None);
                }
                return Err(err).context("failed to read from socket");
            }
        };

        Ok(Some(result))
    }

    async fn connect<A: ToSocketAddrs + Send>(addr: A) -> anyhow::Result<Self> {
        let session = TcpStream::connect(addr)
            .await
            .context("failed to connect to ")?;
        Ok(Self { session })
    }
}

///
/// A session between a client and a torrent server or a peer
///
/// All sent messages are encoded using protobuf with 4 bytes of message size prepended in little endian
#[derive(Debug)]
pub struct ConnectedSession<S> {
    socket_wrapper: S,
}

impl<S> ConnectedSession<S> {
    pub fn with_socket(socket_wrapper: S) -> Self {
        Self { socket_wrapper }
    }
}

impl<S: SocketWrapper> ConnectedSession<S> {
    #[instrument(skip(addr))]
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> anyhow::Result<Self> {
        let host = lookup_host(addr)
            .await
            .context("failed to lookup host")?
            .next()
            .context("no hosts found")?;
        let socket_wrapper = S::connect(host).await?;
        Ok(Self { socket_wrapper })
    }

    pub async fn send_msg<T: Message>(&mut self, message: &T) -> anyhow::Result<()> {
        let message_size = message.encoded_len();
        if message_size > u32::MAX as usize {
            anyhow::bail!("message size ({}) too large", message_size);
        }

        let mut buffer = Vec::with_capacity(message_size + 4);
        buffer.extend_from_slice(&(message_size as u32).to_le_bytes());
        buffer.extend_from_slice(message.encode_to_vec().as_slice());

        self.socket_wrapper
            .send(&buffer)
            .await
            .context("failed to send message")
    }

    pub async fn recv_msg<T: Message + Default>(&mut self) -> anyhow::Result<Option<T>> {
        let mut message_size = [0u8; 4];
        let result = self
            .socket_wrapper
            .recv(&mut message_size)
            .await
            .context("failed to read message size")?;
        if result.is_none() {
            // Socket was closed
            return Ok(None);
        }

        let message_size = u32::from_le_bytes(message_size) as usize;
        let mut buffer = vec![0u8; message_size];

        self.socket_wrapper
            .recv(&mut buffer)
            .await
            .context("failed to read message")?;

        let message = T::decode(buffer.as_slice()).context("failed to decode message")?;
        Ok(Some(message))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    mod client_proto {
        include!(concat!(env!("OUT_DIR"), "/torrent.client.rs"));
    }

    struct MockSocketWrapper {
        buffer: Vec<u8>,
    }

    impl MockSocketWrapper {
        fn new() -> Self {
            Self { buffer: Vec::new() }
        }
    }

    impl SocketWrapper for MockSocketWrapper {
        async fn send(&mut self, buffer: &[u8]) -> anyhow::Result<()> {
            self.buffer.extend_from_slice(buffer);
            Ok(())
        }

        async fn recv(&mut self, buffer: &mut [u8]) -> anyhow::Result<Option<usize>> {
            let bytes_to_read = std::cmp::min(buffer.len(), self.buffer.len());
            buffer[..bytes_to_read].copy_from_slice(&self.buffer[..bytes_to_read]);
            self.buffer.drain(..bytes_to_read);
            Ok(Some(bytes_to_read))
        }

        async fn connect<A: ToSocketAddrs + Send>(_addr: A) -> anyhow::Result<Self> {
            Ok(Self { buffer: Vec::new() })
        }
    }

    fn get_mock_session() -> ConnectedSession<MockSocketWrapper> {
        ConnectedSession {
            socket_wrapper: MockSocketWrapper::new(),
        }
    }

    #[tokio::test]
    async fn test_send_msg() {
        let mut session = get_mock_session();

        let msg = client_proto::DownloadFile {
            filename: "test".to_string(),
        };

        session.send_msg(&msg).await.unwrap();

        let received_msg: client_proto::DownloadFile = session.recv_msg().await.unwrap().unwrap();
        assert_eq!(received_msg.filename, "test");
    }

    #[tokio::test]
    async fn test_sent_messages_include_size() {
        let mut session = get_mock_session();

        let msg = client_proto::DownloadFile {
            filename: "test".to_string(),
        };

        let expected_encoded = msg.encode_to_vec();
        let expected_len = expected_encoded.len();

        session.send_msg(&msg).await.unwrap();

        let mut buffer = vec![0u8; expected_len + 4];
        SocketWrapper::recv(&mut session.socket_wrapper, &mut buffer)
            .await
            .unwrap();

        let received_size =
            u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
        assert_eq!(received_size, expected_len);
        let encoded_msg = &buffer[4..];
        assert_eq!(encoded_msg, expected_encoded.as_slice());
    }
}
