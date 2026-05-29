// Copyright 2026 David Johnson, G4DPZ, AMSAT-UK
// SPDX-License-Identifier: Apache-2.0

/*!
LTP Convergence Layer Adapter for the Bundle Protocol.

This crate implements the Licklider Transmission Protocol (LTP, RFC 5326)
Convergence Layer Adapter for the Hardy DTN router. It integrates the
[`hardy-ltp`] protocol engine with the BPA via the [`hardy_bpa::cla::Cla`]
trait, providing UDP transport, span management, bundle aggregation, rate
control, and observability.

# Key modules

- [`cla`] — CLA trait implementation (`LtpCla`).
- [`span`] — Per-link state: sessions, aggregation buffer, rate control.
- [`engine`] — UDP receive loop and segment dispatch.
- [`config`] — Configuration types (`Config`, `SpanConfig`).
*/

pub mod block;
pub mod cla;
pub mod config;
pub mod engine;
pub mod span;
