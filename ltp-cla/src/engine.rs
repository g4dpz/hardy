//! UDP receive loop and segment dispatch.
//!
//! Spawns a tokio task that reads UDP datagrams, decodes LTP segments,
//! and routes them to the appropriate export or import session.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use hardy_async::CancellationToken;
use hardy_bpa::cla::Sink;
use hardy_ltp::segment::{self, Segment};
use tokio::net::UdpSocket;
use tracing::{debug, trace, warn};

use crate::span::Span;

/// Maximum UDP datagram size we will attempt to read.
const MAX_DATAGRAM_SIZE: usize = 65536;

/// Run the UDP receive loop.
///
/// Reads one UDP datagram per iteration, decodes the LTP segment header,
/// and routes the segment to the appropriate span's import or export session.
///
/// The receive buffer is a reusable stack allocation. After each recv, the
/// received bytes are copied into a `Bytes` handle so that `segment::decode()`
/// can extract data segment payloads via zero-copy `split_to()` instead of
/// allocating a new buffer for each payload.
///
/// This function runs until the `cancel_token` is triggered.
pub(crate) async fn run_receive_loop(
    socket: Arc<UdpSocket>,
    spans: Arc<HashMap<u64, Arc<Span>>>,
    sink: Arc<dyn Sink>,
    cancel_token: CancellationToken,
) {
    let mut buf = vec![0u8; MAX_DATAGRAM_SIZE];

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                debug!("LTP engine: receive loop cancelled");
                break;
            }
            result = socket.recv_from(&mut buf) => {
                match result {
                    Ok((len, src)) => {
                        trace!(
                            bytes = len,
                            source = %src,
                            "LTP engine: received datagram"
                        );
                        // Wrap received bytes in Bytes so that segment::decode()
                        // can use zero-copy split_to() for data payloads instead
                        // of allocating via copy_to_bytes() on a &[u8] cursor.
                        let datagram = Bytes::copy_from_slice(&buf[..len]);
                        handle_datagram(datagram, &spans, &sink).await;
                    }
                    Err(e) => {
                        warn!(error = %e, "LTP engine: UDP recv error, continuing");
                    }
                }
            }
        }
    }
}

/// Process a single received UDP datagram.
///
/// Decodes the segment from a `Bytes` buffer, enabling zero-copy extraction
/// of data segment payloads (via `Buf::copy_to_bytes()` which becomes a
/// reference-counted `split_to()` on `Bytes`). Routes the decoded segment
/// to the appropriate session handler based on segment type and engine ID.
async fn handle_datagram(data: Bytes, spans: &HashMap<u64, Arc<Span>>, _sink: &Arc<dyn Sink>) {
    // Decode the segment from the Bytes buffer.
    // Because Bytes implements Buf with zero-copy copy_to_bytes() (via split_to),
    // data segment payloads are extracted without an additional allocation.
    let mut cursor = data;
    let segment = match segment::decode(&mut cursor) {
        Ok(seg) => seg,
        Err(e) => {
            warn!(error = %e, "LTP engine: failed to decode segment, dropping");
            metrics::counter!("ltp.segments.rx.malformed").increment(1);
            return;
        }
    };

    // Route based on segment type.
    match &segment {
        Segment::Data {
            session_id,
            segment_type,
            client_service_id,
            offset,
            data,
            checkpoint,
            ..
        } => {
            // Data segments (types 0-4, 7) are routed to import sessions.
            let engine_id = session_id.engine_id;
            let span = match spans.get(&engine_id) {
                Some(s) => s,
                None => {
                    warn!(
                        engine_id,
                        session_number = session_id.session_number,
                        segment_type = ?segment_type,
                        "LTP engine: data segment from unknown engine ID, dropping"
                    );
                    return;
                }
            };
            span.on_import_data_segment(
                session_id.session_number,
                *segment_type,
                *client_service_id,
                *offset,
                data,
                checkpoint.clone(),
            )
            .await;
        }

        Segment::Report {
            session_id,
            report_serial,
            checkpoint_serial,
            upper_bound,
            lower_bound,
            claims,
            ..
        } => {
            // Report segments (type 8) are routed to export sessions.
            let engine_id = session_id.engine_id;
            let span = match spans.get(&engine_id) {
                Some(s) => s,
                None => {
                    warn!(
                        engine_id,
                        session_number = session_id.session_number,
                        "LTP engine: report segment from unknown engine ID, dropping"
                    );
                    return;
                }
            };
            span.on_export_report(
                session_id.session_number,
                *report_serial,
                *checkpoint_serial,
                *upper_bound,
                *lower_bound,
                claims,
            )
            .await;
        }

        Segment::ReportAck {
            session_id,
            report_serial,
            ..
        } => {
            // Report-Ack segments (type 9) are routed to import sessions.
            let engine_id = session_id.engine_id;
            let span = match spans.get(&engine_id) {
                Some(s) => s,
                None => {
                    warn!(
                        engine_id,
                        session_number = session_id.session_number,
                        "LTP engine: report-ack from unknown engine ID, dropping"
                    );
                    return;
                }
            };
            span.on_import_report_ack(session_id.session_number, *report_serial)
                .await;
        }

        Segment::Cancel {
            session_id,
            direction,
            reason,
            ..
        } => {
            let engine_id = session_id.engine_id;
            let span = match spans.get(&engine_id) {
                Some(s) => s,
                None => {
                    warn!(
                        engine_id,
                        session_number = session_id.session_number,
                        "LTP engine: cancel segment from unknown engine ID, dropping"
                    );
                    return;
                }
            };

            match direction {
                // Cancel from sender (type 12) → import session handling.
                hardy_ltp::session::CancelDirection::FromSender => {
                    span.on_import_cancel_from_sender(session_id.session_number, *reason)
                        .await;
                }
                // Cancel from receiver (type 14) → export session handling.
                hardy_ltp::session::CancelDirection::FromReceiver => {
                    span.on_export_cancel_from_receiver(session_id.session_number, *reason)
                        .await;
                }
            }
        }

        Segment::CancelAck {
            session_id,
            direction,
            ..
        } => {
            let engine_id = session_id.engine_id;
            let span = match spans.get(&engine_id) {
                Some(s) => s,
                None => {
                    warn!(
                        engine_id,
                        session_number = session_id.session_number,
                        "LTP engine: cancel-ack from unknown engine ID, dropping"
                    );
                    return;
                }
            };

            match direction {
                hardy_ltp::session::CancelDirection::FromSender => {
                    // Check if this is a ping response (session number 0).
                    if session_id.session_number == crate::span::Span::PING_SESSION_NUMBER {
                        span.on_ping_cancel_ack_received();
                    } else {
                        trace!(
                            engine_id,
                            session_number = session_id.session_number,
                            "LTP engine: received cancel-ack-to-sender"
                        );
                    }
                }
                hardy_ltp::session::CancelDirection::FromReceiver => {
                    trace!(
                        engine_id,
                        session_number = session_id.session_number,
                        "LTP engine: received cancel-ack-to-receiver"
                    );
                }
            }
        }
    }
}
