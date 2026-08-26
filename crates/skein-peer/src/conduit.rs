use iroh::EndpointId;
use iroh_blobs::store::fs::FsStore;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;

use crate::error::PeerError;
use crate::iroh_transport::IrohTransport;
use crate::tcp_transport::{TcpConnection, TcpTransport};
use crate::transport::Transport;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ConduitPeerId {
    Iroh(EndpointId),
    Tcp(SocketAddr),
}

impl Display for ConduitPeerId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ConduitPeerId::Iroh(id) => write!(f, "{id}"),
            ConduitPeerId::Tcp(addr) => write!(f, "{addr}"),
        }
    }
}

impl FromStr for ConduitPeerId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(addr) = s.parse::<SocketAddr>() {
            return Ok(ConduitPeerId::Tcp(addr));
        }
        if let Ok(endpoint_id) = s.parse::<EndpointId>() {
            return Ok(ConduitPeerId::Iroh(endpoint_id));
        }
        Err(format!(
            "Cannot parse '{s}' as either SocketAddr or EndpointId"
        ))
    }
}

impl From<EndpointId> for ConduitPeerId {
    fn from(id: EndpointId) -> Self {
        ConduitPeerId::Iroh(id)
    }
}

impl From<SocketAddr> for ConduitPeerId {
    fn from(addr: SocketAddr) -> Self {
        ConduitPeerId::Tcp(addr)
    }
}

#[derive(Clone, Debug)]
pub enum ConduitConnection {
    Iroh(iroh::endpoint::Connection),
    Tcp(TcpConnection),
}

#[derive(Clone, Debug)]
pub enum Conduit {
    Iroh(IrohTransport),
    Tcp(TcpTransport),
}

impl Conduit {
    pub async fn iroh(store_path: impl AsRef<Path>) -> Result<Self, PeerError> {
        let transport = IrohTransport::new(store_path).await?;
        Ok(Conduit::Iroh(transport))
    }

    pub async fn tcp(
        bind_addr: impl AsRef<str>,
        store_path: impl AsRef<Path>,
    ) -> Result<Self, PeerError> {
        let transport = TcpTransport::bind(bind_addr.as_ref(), store_path).await?;
        Ok(Conduit::Tcp(transport))
    }

    pub fn store(&self) -> FsStore {
        match self {
            Conduit::Iroh(t) => t.store(),
            Conduit::Tcp(t) => t.store(),
        }
    }

    pub fn id(&self) -> ConduitPeerId {
        Transport::id(self)
    }

    pub fn remote_id(&self, conn: &ConduitConnection) -> ConduitPeerId {
        Transport::remote_id(self, conn)
    }

    pub async fn connect(&self, peer: ConduitPeerId) -> Result<ConduitConnection, PeerError> {
        Transport::connect(self, peer).await
    }

    pub async fn accept(&self) -> Result<ConduitConnection, PeerError> {
        Transport::accept(self).await
    }

    pub async fn send(&self, conn: &ConduitConnection, data: &[u8]) -> Result<(), PeerError> {
        Transport::send(self, conn, data).await
    }

    pub async fn receive(&self, conn: &ConduitConnection) -> Result<Vec<u8>, PeerError> {
        Transport::receive(self, conn).await
    }

    pub async fn close(&self) {
        Transport::close(self).await
    }
}

#[async_trait::async_trait]
impl Transport for Conduit {
    type PeerId = ConduitPeerId;
    type Connection = ConduitConnection;

    fn id(&self) -> Self::PeerId {
        match self {
            Conduit::Iroh(t) => ConduitPeerId::Iroh(t.id()),
            Conduit::Tcp(t) => ConduitPeerId::Tcp(t.id()),
        }
    }

    fn remote_id(&self, conn: &Self::Connection) -> Self::PeerId {
        match (self, conn) {
            (Conduit::Iroh(t), ConduitConnection::Iroh(c)) => ConduitPeerId::Iroh(t.remote_id(c)),
            (Conduit::Tcp(t), ConduitConnection::Tcp(c)) => ConduitPeerId::Tcp(t.remote_id(c)),
            _ => panic!("Connection and transport mismatch"),
        }
    }

    async fn connect(&self, peer: Self::PeerId) -> Result<Self::Connection, PeerError> {
        match (self, peer) {
            (Conduit::Iroh(t), ConduitPeerId::Iroh(id)) => {
                let conn = t.connect(id).await?;
                Ok(ConduitConnection::Iroh(conn))
            }
            (Conduit::Tcp(t), ConduitPeerId::Tcp(addr)) => {
                let conn = t.connect(addr).await?;
                Ok(ConduitConnection::Tcp(conn))
            }
            (Conduit::Iroh(_), ConduitPeerId::Tcp(addr)) => Err(PeerError::TransportMismatch(
                format!("Attempted to connect to TCP address {addr} using Iroh transport"),
            )),
            (Conduit::Tcp(_), ConduitPeerId::Iroh(id)) => Err(PeerError::TransportMismatch(
                format!("Attempted to connect to Iroh EndpointId {id} using TCP transport"),
            )),
        }
    }

    async fn accept(&self) -> Result<Self::Connection, PeerError> {
        match self {
            Conduit::Iroh(t) => {
                let conn = t.accept().await?;
                Ok(ConduitConnection::Iroh(conn))
            }
            Conduit::Tcp(t) => {
                let conn = t.accept().await?;
                Ok(ConduitConnection::Tcp(conn))
            }
        }
    }

    async fn send(&self, conn: &Self::Connection, data: &[u8]) -> Result<(), PeerError> {
        match (self, conn) {
            (Conduit::Iroh(t), ConduitConnection::Iroh(c)) => t.send(c, data).await,
            (Conduit::Tcp(t), ConduitConnection::Tcp(c)) => t.send(c, data).await,
            _ => Err(PeerError::TransportMismatch(
                "Connection type does not match transport type".to_string(),
            )),
        }
    }

    async fn receive(&self, conn: &Self::Connection) -> Result<Vec<u8>, PeerError> {
        match (self, conn) {
            (Conduit::Iroh(t), ConduitConnection::Iroh(c)) => t.receive(c).await,
            (Conduit::Tcp(t), ConduitConnection::Tcp(c)) => t.receive(c).await,
            _ => Err(PeerError::TransportMismatch(
                "Connection type does not match transport type".to_string(),
            )),
        }
    }

    async fn close(&self) {
        match self {
            Conduit::Iroh(t) => t.close().await,
            Conduit::Tcp(t) => t.close().await,
        }
    }
}
