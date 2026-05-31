use hardy_bpa::bpa::{Bpa, BpaRegistration};
use hardy_bpa::routes::{Action, StaticRoutingAgent};
use hardy_bpv7::eid::{Eid, IpnNodeId, NodeId};
use rand::RngExt;
use std::path::Path;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Establish a TCPCLv4 session with a remote BPA.
///
/// Creates a minimal local BPA instance (no status reports, no admin services),
/// registers a default route forwarding all bundles via the remote BPA, creates
/// a TCPCLv4 CLA in client-only mode (no listener), and connects to the peer.
///
/// # Arguments
///
/// * `peer` - The peer address as `host:port` string
/// * `node_id` - Optional node ID for this tool; if `None`, a random IPN node ID is generated
/// * `tls_insecure` - Accept self-signed TLS certificates
/// * `tls_ca` - Optional path to a CA certificate directory for TLS verification
///
/// # Returns
///
/// An `Arc<Bpa>` representing the minimal local BPA with an active TCPCLv4 session.
pub async fn establish_session(
    peer: &str,
    node_id: Option<NodeId>,
    tls_insecure: bool,
    tls_ca: Option<&Path>,
) -> anyhow::Result<Arc<Bpa>> {
    // 1. Determine node ID (provided or random)
    let node_id = node_id.unwrap_or_else(random_node_id);
    let node_ids = [node_id.clone()].as_slice().try_into().unwrap();

    // 2. Create minimal BPA (no status reports, no services)
    let bpa = Arc::new(
        Bpa::builder()
            .status_reports(false)
            .node_ids(node_ids)
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to build BPA: {e}"))?,
    );

    // 3. Register default route: forward everything via the remote BPA.
    // We use ipn:N.1 (service 1) as the Via target instead of ipn:N.0
    // because ipn:N.0 is the admin endpoint (which resolves to local delivery).
    let via_eid = node_id_to_service_eid(&node_id, 1);
    bpa.register_routing_agent(
        "file-transfer".to_string(),
        Arc::new(StaticRoutingAgent::new(&[(
            "*:**".parse().unwrap(),
            Action::Via(via_eid),
            100,
        )])),
    )
    .await
    .map_err(|e| anyhow::anyhow!("Failed to register default routes: {e}"))?;

    // 4. Start the BPA (no storage recovery needed)
    bpa.start(false);

    // 5. Create TCPCLv4 CLA (no listener, client-only)
    let mut tcpclv4_config = hardy_tcpclv4::config::Config {
        address: None, // No listener — we only connect outbound
        session_defaults: hardy_tcpclv4::config::SessionConfig {
            require_tls: false,
            ..Default::default()
        },
        ..Default::default()
    };

    // Configure TLS if --tls-insecure or --tls-ca is specified
    if tls_insecure || tls_ca.is_some() {
        let mut tls_config = hardy_tcpclv4::config::TlsConfig::default();
        if tls_insecure {
            tls_config.debug.accept_self_signed = true;
        }
        if let Some(ca_dir) = tls_ca {
            if !ca_dir.exists() {
                return Err(anyhow::anyhow!(
                    "CA bundle directory not found: {}",
                    ca_dir.display()
                ));
            }
            if !ca_dir.is_dir() {
                return Err(anyhow::anyhow!(
                    "CA bundle must be a directory, not a file: {}",
                    ca_dir.display()
                ));
            }
            tls_config.ca_certs = Some(ca_dir.to_path_buf());
        }
        tcpclv4_config.tls = Some(tls_config);
        tcpclv4_config.session_defaults.require_tls = true;
    }

    let cla = Arc::new(
        hardy_tcpclv4::Cla::new(&tcpclv4_config)
            .map_err(|e| anyhow::anyhow!("Failed to create TCPCLv4 CLA: {e}"))?,
    );

    // 6. Register CLA with BPA
    bpa.register_cla("tcpclv4".to_string(), cla.clone(), None)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to register CLA: {e}"))?;

    // 7. Connect to remote BPA
    let peer_addr: std::net::SocketAddr = peer
        .parse()
        .map_err(|e| anyhow::anyhow!("Failed to parse peer address '{peer}': {e}"))?;

    cla.connect(&peer_addr)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to connect to {peer_addr}: {e}"))?;

    // 8. Wait for session registration to complete (SESS_INIT exchange + add_peer).
    // The TCPCLv4 session registers the remote BPA as a peer with its node ID.
    // TODO: Replace with proper wait/notification mechanism
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // 9. Return the BPA
    Ok(bpa)
}

/// Generate a random IPN node ID for use when `--node-id` is not provided.
///
/// Uses a random allocator_id in the private range and a random non-zero node_number
/// to avoid collisions with real BPA node IDs.
fn random_node_id() -> NodeId {
    let mut rng = rand::rng();
    NodeId::Ipn(IpnNodeId {
        allocator_id: rng.random_range(0x40000000..0x80000000),
        node_number: rng.random_range(1..u32::MAX),
    })
}

/// Convert a NodeId to an EID with the given service number.
///
/// Used to construct the Via target for the default route.
fn node_id_to_service_eid(node_id: &NodeId, service_number: u32) -> Eid {
    match node_id {
        NodeId::Ipn(fqnn) => Eid::Ipn {
            fqnn: *fqnn,
            service_number,
        },
        NodeId::Dtn(node_name) => Eid::Dtn {
            node_name: node_name.clone(),
            service_name: format!("svc-{service_number}").into(),
        },
        NodeId::LocalNode => Eid::LocalNode(service_number),
    }
}

/// Set up a signal handler for graceful shutdown.
///
/// Creates a [`CancellationToken`] and spawns a background task that listens
/// for SIGINT (Ctrl+C) and SIGTERM signals:
///
/// - **First signal**: cancels the token, allowing in-progress work to complete
///   gracefully (finish current transfer, send SESS_TERM, close connection).
/// - **Second signal**: calls `std::process::exit(130)` for immediate termination.
///
/// The returned token should be checked via `token.is_cancelled()` or awaited
/// via `token.cancelled()` in the caller's main loop.
///
/// # Example
///
/// ```no_run
/// use crate::session::setup_signal_handler;
///
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// let token = setup_signal_handler();
///
/// // Main work loop
/// loop {
///     if token.is_cancelled() {
///         break;
///     }
///     // ... do work ...
/// }
/// # });
/// ```
pub fn setup_signal_handler() -> CancellationToken {
    let token = CancellationToken::new();
    let token_clone = token.clone();

    tokio::spawn(async move {
        // Wait for the first signal
        wait_for_signal().await;
        eprintln!("\nReceived interrupt, shutting down gracefully...");
        token_clone.cancel();

        // Wait for the second signal — immediate exit
        wait_for_signal().await;
        eprintln!("\nReceived second interrupt, exiting immediately.");
        std::process::exit(130);
    });

    token
}

/// Wait for a single SIGINT or SIGTERM signal.
#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{signal, SignalKind};

    let mut sigterm =
        signal(SignalKind::terminate()).expect("Failed to register SIGTERM handler");

    tokio::select! {
        _ = sigterm.recv() => {}
        result = tokio::signal::ctrl_c() => {
            result.expect("Failed to listen for CTRL+C");
        }
    }
}

/// Wait for a single Ctrl+C signal (non-Unix platforms).
#[cfg(not(unix))]
async fn wait_for_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for CTRL+C");
}

/// Perform graceful shutdown of the BPA.
///
/// This shuts down the BPA's internal subsystems (CLA connections, dispatcher,
/// storage, services) in the correct order. The TCPCLv4 CLA shutdown sends
/// SESS_TERM to all active sessions before closing TCP connections.
pub async fn shutdown(bpa: Arc<Bpa>) {
    bpa.shutdown().await;
}
