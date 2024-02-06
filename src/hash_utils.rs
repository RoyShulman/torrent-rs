use anyhow::Context as _;
use ring::digest::{Context, SHA256};
use tokio::sync::oneshot;

pub async fn sha256_hash_chunk(buffer: Vec<u8>) -> anyhow::Result<Vec<u8>> {
    let (sender, recv) = oneshot::channel();

    rayon::spawn(move || {
        let mut context = Context::new(&SHA256);
        context.update(&buffer);
        let digest = context.finish();
        if let Err(_) = sender.send(digest.as_ref().to_vec()) {
            tracing::error!("Failed to send hash result");
        }
    });

    recv.await.context("failed to receive hash result")
}
