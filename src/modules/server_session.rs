use anyhow::Context;
use prost::Message;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

///
/// Trait for a session with a torrent server
/// Used to abstract the internal sending and receiving of buffers.
pub trait ServerSocketWrapper {
    async fn connect(addr: &str) -> anyhow::Result<Self>
    where
        Self: Sized;
    async fn send(&mut self, buffer: &[u8]) -> anyhow::Result<()>;
    async fn recv(&mut self, buffer: &mut [u8]) -> anyhow::Result<usize>;
}

#[derive(Debug)]
pub struct TokioServerSocketWrapper {
    session: TcpStream,
}

impl ServerSocketWrapper for TokioServerSocketWrapper {
    async fn send(&mut self, buffer: &[u8]) -> anyhow::Result<()> {
        self.session
            .write_all(buffer)
            .await
            .context("failed to write to socket")
    }

    async fn recv(&mut self, buffer: &mut [u8]) -> anyhow::Result<usize> {
        self.session
            .read_exact(buffer)
            .await
            .context("failed to read from socket")
    }

    async fn connect(addr: &str) -> anyhow::Result<Self> {
        let session = TcpStream::connect(addr)
            .await
            .context("failed to connect to server")?;
        Ok(Self { session })
    }
}

///
/// A session between a client and a torrent server
///
/// All sent messages are encoded using protobuf with 4 bytes of message size prepended in little endian
#[derive(Debug)]
pub struct ClientServerSession<S> {
    socket_wrapper: S,
}

impl<S: ServerSocketWrapper> ClientServerSession<S> {
    pub async fn connect(addr: &str) -> anyhow::Result<Self> {
        let socket_wrapper = S::connect(addr).await?;
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

    pub async fn recv_msg<T: Message + Default>(&mut self) -> anyhow::Result<T> {
        let mut message_size = [0u8; 4];
        self.socket_wrapper
            .recv(&mut message_size)
            .await
            .context("failed to read message size")?;

        let message_size = u32::from_le_bytes(message_size) as usize;
        let mut buffer = vec![0u8; message_size];

        self.socket_wrapper
            .recv(&mut buffer)
            .await
            .context("failed to read message")?;

        Message::decode(buffer.as_slice()).context("failed to decode message")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    mod client_proto {
        include!(concat!(env!("OUT_DIR"), "/torrent.client.rs"));
    }

    struct MockServerSocketWrapper {
        buffer: Vec<u8>,
    }

    impl MockServerSocketWrapper {
        fn new() -> Self {
            Self { buffer: Vec::new() }
        }
    }

    impl ServerSocketWrapper for MockServerSocketWrapper {
        async fn send(&mut self, buffer: &[u8]) -> anyhow::Result<()> {
            self.buffer.extend_from_slice(buffer);
            Ok(())
        }

        async fn recv(&mut self, buffer: &mut [u8]) -> anyhow::Result<usize> {
            let bytes_to_read = std::cmp::min(buffer.len(), self.buffer.len());
            buffer[..bytes_to_read].copy_from_slice(&self.buffer[..bytes_to_read]);
            self.buffer.drain(..bytes_to_read);
            Ok(bytes_to_read)
        }

        async fn connect(_addr: &str) -> anyhow::Result<Self> {
            Ok(Self { buffer: Vec::new() })
        }
    }

    fn get_mock_session() -> ClientServerSession<MockServerSocketWrapper> {
        ClientServerSession {
            socket_wrapper: MockServerSocketWrapper::new(),
        }
    }

    #[tokio::test]
    async fn test_send_msg() {
        let mut session = get_mock_session();

        let msg = client_proto::DownloadFile {
            filename: "test".to_string(),
        };

        session.send_msg(&msg).await.unwrap();

        let received_msg: client_proto::DownloadFile = session.recv_msg().await.unwrap();
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
        session.socket_wrapper.recv(&mut buffer).await.unwrap();

        let received_size =
            u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;
        assert_eq!(received_size, expected_len);
        let encoded_msg = &buffer[4..];
        assert_eq!(encoded_msg, expected_encoded.as_slice());
    }
}
