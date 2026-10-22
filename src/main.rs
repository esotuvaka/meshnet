use clap::{Parser, Subcommand};
use iroh::{Endpoint, EndpointAddr, EndpointId, endpoint::presets};
use n0_error::StdResultExt;
use std::sync::Arc;
use std::{net::Ipv4Addr, str::FromStr};
use tun_rs::DeviceBuilder;

const ALPN: &[u8] = b"meshnet/tunnel/0";

#[derive(Parser)]
#[command(name = "meshnet", about = "P2P virtual mesh network")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Start listening for a peer connection
    Listen {
        /// Virtual IP address for this node (e.g., 10.0.0.1)
        #[arg(long)]
        ip: Ipv4Addr,
        /// Subnet mask prefix length
        #[arg(long, default_value = "24")]
        mask: u8,
    },
    /// Connect to a listening peer
    Connect {
        /// Virtual IP address for this node (e.g., 10.0.0.2)
        #[arg(long)]
        ip: Ipv4Addr,
        /// Subnet mask prefix length
        #[arg(long, default_value = "24")]
        mask: u8,
        /// Remote peer's EndpointAddr ticket string
        addr: String,
    },
}

async fn create_tun(ip: Ipv4Addr, mask: u8) -> std::io::Result<tun_rs::AsyncDevice> {
    let dev = DeviceBuilder::new()
        .ipv4(ip, mask, None)
        .mtu(1400)
        .build_async()?;
    println!("Created TUN device: {:?}", dev.name());
    Ok(dev)
}

async fn run_listen(ip: Ipv4Addr, mask: u8) -> Result<(), Box<dyn std::error::Error>> {
    let tun_dev = Arc::new(create_tun(ip, mask).await?);

    // Create an iroh endpoint and register our custom protocol
    let endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await?;

    // Wait until connected to a relay server
    endpoint.online().await;
    println!("Listening. Share this address with your peer:");
    println!("{:?}", endpoint.addr());

    // Block until a peer connects
    let incoming = endpoint.accept().await.ok_or("No incoming connection")?;
    let conn = incoming.await.anyerr()?;
    println!("Peer connected: {}", conn.remote_id());

    // Accept the bidirectional stream from the connector
    let (send_stream, recv_stream) = conn.accept_bi().await.anyerr()?;

    // Bridge the TUN device and iroh stream
    forward_packets(tun_dev, send_stream, recv_stream).await;

    // Clean up the connection and endpoint
    conn.close(0u32.into(), b"done");
    endpoint.close().await;
    Ok(())
}

async fn run_connect(
    ip: Ipv4Addr,
    mask: u8,
    addr_str: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let tun_dev = Arc::new(create_tun(ip, mask).await?);

    // Create an iroh endpoint (no ALPN needed for the connecting side)
    let endpoint = Endpoint::bind(presets::N0).await?;
    endpoint.online().await;

    // Parse the remote peer's ticket and connect
    let endpoint_id: EndpointId = EndpointId::from_str(addr_str)?;
    let addr = EndpointAddr::from(endpoint_id);
    println!("Connecting to peer...");
    let conn = endpoint.connect(addr, ALPN).await?;
    println!("Connected to: {}", conn.remote_id());

    // Open a bidirectional stream to the listener
    let (send_stream, recv_stream) = conn.open_bi().await.anyerr()?;

    // Bridge the TUN device and iroh stream
    forward_packets(tun_dev, send_stream, recv_stream).await;

    // Clean up the connection and endpoint
    conn.close(0u32.into(), b"done");
    endpoint.close().await;
    Ok(())
}

async fn forward_packets(
    tun_dev: Arc<tun_rs::AsyncDevice>,
    mut send_stream: iroh::endpoint::SendStream,
    mut recv_stream: iroh::endpoint::RecvStream,
) {
    // Clone the TUN device for the receive direction
    // (AsyncDevice uses Arc internally on Linux, so this is cheap)
    let tun_send = tun_dev.clone();

    tokio::select! {
        // Direction 1: TUN -> iroh (outbound packets)
        result = async {
            let mut buf = vec![0u8; 1500];
            loop {
                // Read a raw IP packet from the TUN device
                let n = tun_dev.recv(&mut buf).await?;
                if n == 0 {
                    continue;
                }
                // Length-prefix: write packet size as 2-byte big-endian u16
                let len = (n as u16).to_be_bytes();
                send_stream.write_all(&len).await.anyerr()?;
                // Write the actual packet data
                send_stream.write_all(&buf[..n]).await.anyerr()?;
            }
            #[allow(unreachable_code)]
            Ok::<(), n0_error::AnyError>(())
        } => {
            if let Err(e) = result {
                eprintln!("TUN -> iroh error: {e}");
            }
        }
        // Direction 2: iroh -> TUN (inbound packets)
        result = async {
            let mut len_buf = [0u8; 2];
            loop {
                // Read the 2-byte length prefix
                recv_stream.read_exact(&mut len_buf).await.anyerr()?;
                let len = u16::from_be_bytes(len_buf) as usize;
                // Read exactly that many bytes for the packet
                let mut pkt_buf = vec![0u8; len];
                recv_stream.read_exact(&mut pkt_buf).await.anyerr()?;
                // Write the packet into the TUN device
                tun_send.send(&pkt_buf).await?;
            }
            #[allow(unreachable_code)]
            Ok::<(), n0_error::AnyError>(())
        } => {
            if let Err(e) = result {
                eprintln!("iroh -> TUN error: {e}");
            }
        }
        // Direction 3: Ctrl+C handler for clean shutdown
        _ = tokio::signal::ctrl_c() => {
            println!("\nShutting down...");
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Command::Listen { ip, mask } => run_listen(ip, mask).await,
        Command::Connect { ip, mask, addr } => run_connect(ip, mask, &addr).await,
    }
}
