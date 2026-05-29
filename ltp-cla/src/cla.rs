//! CLA trait implementation for LTP.
//!
//! Implements [`hardy_bpa::cla::Cla`] to integrate LTP with the BPA.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use hardy_async::CancellationToken;
use hardy_bpa::async_trait;
use hardy_bpa::cla::{Cla, ClaAddress, ClaAddressType, ForwardBundleResult, Sink};
use tokio::net::UdpSocket;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::span::Span;

/// Internal state initialized during `on_register`.
#[allow(dead_code)]
struct Inner {
    /// Communication channel back to the BPA.
    sink: Arc<dyn Sink>,
    /// Bound UDP socket for sending and receiving LTP segments.
    socket: Arc<UdpSocket>,
    /// Map of remote engine ID → Span state.
    spans: HashMap<u64, Arc<Span>>,
    /// Cancellation token for the receive task.
    cancel_token: CancellationToken,
}

/// LTP Convergence Layer Adapter.
///
/// Implements the [`Cla`] trait to integrate LTP with the Hardy BPA.
/// Each instance manages a single UDP socket and a set of pre-configured
/// spans (links to remote LTP engines).
pub struct LtpCla {
    /// CLA configuration.
    config: Config,
    /// Internal state, initialized on registration.
    inner: OnceLock<Inner>,
}

impl LtpCla {
    /// Create a new LTP CLA from configuration.
    pub fn new(config: Config) -> Self {
        Self {
            config,
            inner: OnceLock::new(),
        }
    }

    /// Get the inner state, if registered.
    fn inner(&self) -> Option<&Inner> {
        self.inner.get()
    }
}

#[async_trait]
impl Cla for LtpCla {
    fn address_type(&self) -> Option<ClaAddressType> {
        // LTP uses Private addresses (engine ID encoded as bytes).
        None
    }

    fn queue_count(&self) -> u32 {
        // Simple FIFO — no priority queues.
        0
    }

    async fn on_register(&self, sink: Box<dyn Sink>, _node_ids: &[hardy_bpv7::eid::NodeId]) {
        // Convert Box<dyn Sink> to Arc<dyn Sink> for sharing.
        let sink: Arc<dyn Sink> = sink.into();

        // Bind the UDP socket.
        let socket = match UdpSocket::bind(self.config.bind).await {
            Ok(s) => Arc::new(s),
            Err(e) => {
                error!(
                    "LTP CLA: failed to bind UDP socket to {}: {e}",
                    self.config.bind
                );
                return;
            }
        };

        info!("LTP CLA: bound UDP socket to {}", self.config.bind);

        // Derive local engine ID (use configured value or default to 0).
        let local_engine_id = self.config.engine_id.unwrap_or(0);

        // Build span map and register peers with the BPA.
        let mut spans = HashMap::new();
        for span_config in &self.config.spans {
            let engine_id = span_config.engine_id;
            let span = Arc::new(Span::new(
                span_config.clone(),
                local_engine_id,
                socket.clone(),
                sink.clone(),
            ));
            spans.insert(engine_id, span);

            // Build the CLA address: Private(engine_id as 8-byte big-endian).
            let addr = ClaAddress::Private(Bytes::copy_from_slice(&engine_id.to_be_bytes()));

            // Parse node IDs from the span configuration strings.
            let mut node_ids = Vec::new();
            for nid_str in &span_config.node_ids {
                match nid_str.parse::<hardy_bpv7::eid::NodeId>() {
                    Ok(nid) => node_ids.push(nid),
                    Err(e) => {
                        warn!(
                            "LTP CLA: failed to parse node ID '{}' for engine {}: {e}",
                            nid_str, engine_id
                        );
                    }
                }
            }

            // Register this span as a peer with the BPA.
            if let Err(e) = sink.add_peer(addr, &node_ids).await {
                warn!(
                    "LTP CLA: failed to register peer for engine {}: {e}",
                    engine_id
                );
            } else {
                debug!("LTP CLA: registered peer for engine {engine_id}");
            }
        }

        // Create cancellation token for the receive task.
        let cancel_token = CancellationToken::new();

        // Spawn the UDP receive loop (engine.rs).
        let rx_socket = socket.clone();
        let rx_cancel = cancel_token.clone();
        let rx_spans = Arc::new(spans.clone());
        let rx_sink = sink.clone();
        tokio::spawn(crate::engine::run_receive_loop(
            rx_socket, rx_spans, rx_sink, rx_cancel,
        ));

        // Store the inner state.
        let _ = self.inner.set(Inner {
            sink,
            socket,
            spans,
            cancel_token,
        });

        info!(
            "LTP CLA: registered with {} span(s)",
            self.config.spans.len()
        );
    }

    async fn on_unregister(&self) {
        if let Some(inner) = self.inner.get() {
            // Signal the receive task to stop.
            inner.cancel_token.cancel();
            debug!("LTP CLA: unregistered, receive task cancelled");
        }
    }

    async fn forward(
        &self,
        _queue: Option<u32>,
        cla_addr: &ClaAddress,
        bundle: Bytes,
    ) -> hardy_bpa::cla::Result<ForwardBundleResult> {
        let inner = match self.inner() {
            Some(inner) => inner,
            None => {
                warn!("LTP CLA: forward called before registration");
                return Ok(ForwardBundleResult::NoNeighbour);
            }
        };

        // Decode engine ID from Private address (8-byte big-endian u64).
        let engine_id = match cla_addr {
            ClaAddress::Private(bytes) if bytes.len() == 8 => {
                u64::from_be_bytes(bytes[..8].try_into().unwrap())
            }
            _ => {
                warn!("LTP CLA: forward called with invalid CLA address: {cla_addr}");
                return Ok(ForwardBundleResult::NoNeighbour);
            }
        };

        // Look up the span for this engine ID.
        let span = match inner.spans.get(&engine_id) {
            Some(span) => span.clone(),
            None => {
                warn!("LTP CLA: no span configured for engine ID {engine_id}");
                return Err(hardy_bpa::cla::Error::Internal(
                    format!("unknown LTP engine ID: {engine_id}").into(),
                ));
            }
        };

        // Append the bundle to the span's aggregation buffer.
        let (flushed, buffer_non_empty) = {
            let mut agg = span.aggregation.lock().unwrap();
            let flushed = agg.append(&bundle);
            // If aggr_time_limit_secs is 0, flush immediately (no aggregation).
            let immediate_flush = if flushed.is_none() && span.config.aggr_time_limit_secs == 0 {
                agg.flush()
            } else {
                None
            };
            let non_empty = !agg.is_empty();
            (flushed.or(immediate_flush), non_empty)
        };

        if let Some(block) = flushed {
            // Size-based flush occurred — cancel any pending timer and create session.
            {
                let mut timer = span.aggr_timer.lock().unwrap();
                if let Some(handle) = timer.take() {
                    handle.abort();
                }
            }
            let span_clone = span.clone();
            tokio::spawn(async move {
                span_clone.create_export_session(block).await;
            });
        } else if buffer_non_empty {
            // Buffer has data but didn't flush — ensure timer is running.
            span.start_aggr_timer_if_needed();
        }

        Ok(ForwardBundleResult::Sent)
    }
}
