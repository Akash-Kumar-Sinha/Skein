use iroh_blobs::store::fs::FsStore;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::{Mutex, Notify};

use crate::Transport;
use crate::error::PeerError;

pub const MAX_TCP_MESSAGE_SIZE: usize = 50 * 1024 * 1024;

pub fn format_size(bytes: usize) -> String {
    if bytes >= 1024 * 1024 {
        format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.2} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}

pub fn render_progress_bar(current: usize, total: usize, start_time: Instant) -> String {
    let width: usize = 30;
    let pct = if total > 0 {
        (current as f64 / total as f64).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let filled = (pct * width as f64).round() as usize;
    let empty = width.saturating_sub(filled);

    let bar: String = "█".repeat(filled) + &"░".repeat(empty);
    let pct_text = format!("{:>5.1}%", pct * 100.0);

    let elapsed = start_time.elapsed().as_secs_f64();
    let speed_str = if elapsed > 0.05 {
        let speed = current as f64 / elapsed;
        format!("{}/s", format_size(speed as usize))
    } else {
        "-- B/s".to_string()
    };

    format!(
        "\r< [{bar}] {pct_text} ({} / {}) {speed_str}",
        format_size(current),
        format_size(total)
    )
}

#[derive(Debug, Clone)]
pub struct TcpConnection {
    local_addr: SocketAddr,
    peer_addr: SocketAddr,
    reader: Arc<Mutex<OwnedReadHalf>>,
    writer: Arc<Mutex<OwnedWriteHalf>>,
}

impl TcpConnection {
    pub fn new(stream: TcpStream) -> Result<Self, PeerError> {
        let local_addr = stream.local_addr()?;
        let peer_addr = stream.peer_addr()?;
        let (read_half, write_half) = stream.into_split();

        Ok(Self {
            local_addr,
            peer_addr,
            reader: Arc::new(Mutex::new(read_half)),
            writer: Arc::new(Mutex::new(write_half)),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    pub async fn send(&self, data: &[u8]) -> Result<(), PeerError> {
        if data.len() > MAX_TCP_MESSAGE_SIZE {
            return Err(PeerError::PayloadTooLarge {
                size: data.len(),
                max: MAX_TCP_MESSAGE_SIZE,
            });
        }

        let mut writer = self.writer.lock().await;
        let len = data.len() as u32;
        writer.write_all(&len.to_be_bytes()).await?;
        writer.write_all(data).await?;
        writer.flush().await?;
        Ok(())
    }

    pub async fn receive(&self) -> Result<Vec<u8>, PeerError> {
        let mut reader = self.reader.lock().await;
        let mut len_bytes = [0u8; 4];
        reader.read_exact(&mut len_bytes).await?;
        let len = u32::from_be_bytes(len_bytes) as usize;

        if len > MAX_TCP_MESSAGE_SIZE {
            return Err(PeerError::PayloadTooLarge {
                size: len,
                max: MAX_TCP_MESSAGE_SIZE,
            });
        }

        let mut buffer = vec![0u8; len];
        let mut read_bytes = 0;
        let show_progress = len >= 1024;
        let start_time = Instant::now();

        while read_bytes < len {
            let chunk_to_read = std::cmp::min(32 * 1024, len - read_bytes);
            let n = reader
                .read(&mut buffer[read_bytes..read_bytes + chunk_to_read])
                .await?;
            if n == 0 {
                return Err(PeerError::ConnectionClosed);
            }
            read_bytes += n;

            if show_progress {
                print!("{}", render_progress_bar(read_bytes, len, start_time));
                let _ = std::io::Write::flush(&mut std::io::stdout());
            }
        }

        if show_progress {
            print!("{}", render_progress_bar(len, len, start_time));
            println!("\n< Download complete");
        }

        Ok(buffer)
    }
}

#[derive(Debug, Clone)]
pub struct TcpTransport {
    local_addr: SocketAddr,
    listener: Arc<Mutex<Option<TcpListener>>>,
    shutdown_notify: Arc<Notify>,
    store: FsStore,
}

impl TcpTransport {
    pub async fn new(store_path: impl AsRef<Path>) -> Result<Self, PeerError> {
        let bind_addr =
            std::env::var("SKEIN_TCP_BIND").unwrap_or_else(|_| "127.0.0.1:0".to_string());
        Self::bind(&bind_addr, store_path).await
    }

    pub async fn bind<A: ToSocketAddrs>(
        addr: A,
        store_path: impl AsRef<Path>,
    ) -> Result<Self, PeerError> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let store = FsStore::load(store_path.as_ref())
            .await
            .map_err(|e| PeerError::BlobStore(e.to_string()))?;

        Ok(Self {
            local_addr,
            listener: Arc::new(Mutex::new(Some(listener))),
            shutdown_notify: Arc::new(Notify::new()),
            store,
        })
    }

    pub fn from_parts(local_addr: SocketAddr, listener: TcpListener, store: FsStore) -> Self {
        Self {
            local_addr,
            listener: Arc::new(Mutex::new(Some(listener))),
            shutdown_notify: Arc::new(Notify::new()),
            store,
        }
    }

    pub fn id(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn remote_id(&self, conn: &TcpConnection) -> SocketAddr {
        conn.peer_addr()
    }

    pub fn store(&self) -> FsStore {
        self.store.clone()
    }

    pub async fn connect(&self, peer: SocketAddr) -> Result<TcpConnection, PeerError> {
        let stream = TcpStream::connect(peer).await?;
        TcpConnection::new(stream)
    }

    pub async fn accept(&self) -> Result<TcpConnection, PeerError> {
        let listener_guard = self.listener.lock().await;
        let listener = listener_guard.as_ref().ok_or(PeerError::TransportClosed)?;

        let (stream, _) = listener.accept().await?;
        TcpConnection::new(stream)
    }

    pub async fn send(&self, conn: &TcpConnection, data: &[u8]) -> Result<(), PeerError> {
        conn.send(data).await
    }

    pub async fn receive(&self, conn: &TcpConnection) -> Result<Vec<u8>, PeerError> {
        conn.receive().await
    }

    pub async fn send_message(&self, conn: &TcpConnection, data: &[u8]) -> Result<(), PeerError> {
        self.send(conn, data).await
    }

    pub async fn receive_message(&self, conn: &TcpConnection) -> Result<Vec<u8>, PeerError> {
        self.receive(conn).await
    }

    pub async fn close(&self) {
        let mut listener = self.listener.lock().await;
        *listener = None;
        self.shutdown_notify.notify_waiters();
    }
}

#[async_trait::async_trait]
impl Transport for TcpTransport {
    type PeerId = SocketAddr;
    type Connection = TcpConnection;

    fn id(&self) -> Self::PeerId {
        self.id()
    }

    fn remote_id(&self, conn: &Self::Connection) -> Self::PeerId {
        self.remote_id(conn)
    }

    async fn connect(&self, peer: Self::PeerId) -> Result<Self::Connection, PeerError> {
        self.connect(peer).await
    }

    async fn accept(&self) -> Result<Self::Connection, PeerError> {
        self.accept().await
    }

    async fn send(&self, conn: &Self::Connection, data: &[u8]) -> Result<(), PeerError> {
        self.send(conn, data).await
    }

    async fn receive(&self, conn: &Self::Connection) -> Result<Vec<u8>, PeerError> {
        self.receive(conn).await
    }

    async fn close(&self) {
        self.close().await;
    }
}
