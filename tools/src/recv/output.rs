use hardy_bpv7::bpsec;
use hardy_bpv7::bundle::ParsedBundle;
use hardy_bpv7::creation_timestamp::CreationTimestamp;
use hardy_bpv7::eid::Eid;

/// A successfully received and parsed bundle.
#[derive(Debug)]
pub struct ReceivedBundle {
    /// The source EID of the bundle (who sent it).
    pub source: Eid,
    /// The creation timestamp from the bundle's primary block.
    pub creation_timestamp: CreationTimestamp,
    /// The extracted payload bytes.
    pub payload: Vec<u8>,
}

/// Extract the payload from a received BPv7 bundle.
///
/// Parses the raw bytes as a BPv7 bundle, validates that it contains a payload block,
/// and returns the source EID, creation timestamp, and payload data.
///
/// Accepts bundles with or without CRC on the payload block.
pub fn extract_payload(bundle_bytes: &[u8]) -> anyhow::Result<ReceivedBundle> {
    let parsed = ParsedBundle::parse(bundle_bytes, bpsec::no_keys)
        .map_err(|e| anyhow::anyhow!("Invalid BPv7 bundle: {e}"))?;

    // The payload block is always block number 1 in BPv7 (RFC 9171)
    let payload_block = parsed
        .bundle
        .blocks
        .get(&1)
        .ok_or_else(|| anyhow::anyhow!("Bundle has no payload block"))?;

    let payload_data = payload_block
        .payload(bundle_bytes)
        .ok_or_else(|| anyhow::anyhow!("Payload block has no data"))?;

    Ok(ReceivedBundle {
        source: parsed.bundle.id.source.clone(),
        creation_timestamp: parsed.bundle.id.timestamp.clone(),
        payload: payload_data.to_vec(),
    })
}

/// Generate a unique output filename from the bundle source EID and creation timestamp.
///
/// The filename incorporates the source EID and creation timestamp to ensure
/// uniqueness. EID characters that are unsafe for filesystems (`:` and `.`) are
/// replaced with `_`.
///
/// Format: `{sanitized_source_eid}_{creation_time_millis}_{sequence_number}`
///
/// Example: `ipn_2_1_1234567890123_0` for a bundle from `ipn:2.1` with
/// creation time 1234567890123ms and sequence number 0.
pub fn output_filename(source: &Eid, timestamp: &CreationTimestamp) -> String {
    let safe_source = source.to_string().replace(':', "_").replace('.', "_");

    let time_millis = timestamp
        .creation_time()
        .map(|t| t.millisecs())
        .unwrap_or(0);

    format!("{}_{}_{}", safe_source, time_millis, timestamp.sequence_number())
}

/// Write a received bundle payload to the appropriate destination.
///
/// If `output_dir` is `Some`, writes the payload to a file in that directory
/// named using `output_filename`. If `output_dir` is `None`, writes the payload
/// to stdout.
pub fn write_payload(
    payload: &[u8],
    source: &Eid,
    timestamp: &CreationTimestamp,
    output_dir: Option<&std::path::Path>,
) -> anyhow::Result<Option<std::path::PathBuf>> {
    use std::io::Write;

    match output_dir {
        Some(dir) => {
            let filename = output_filename(source, timestamp);
            let path = dir.join(&filename);
            std::fs::write(&path, payload)
                .map_err(|e| anyhow::anyhow!("Failed to write {}: {e}", path.display()))?;
            Ok(Some(path))
        }
        None => {
            let mut stdout = std::io::stdout().lock();
            stdout
                .write_all(payload)
                .map_err(|e| anyhow::anyhow!("Failed to write to stdout: {e}"))?;
            stdout
                .flush()
                .map_err(|e| anyhow::anyhow!("Failed to flush stdout: {e}"))?;
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hardy_bpv7::builder::Builder;
    use hardy_bpv7::dtn_time::DtnTime;
    use std::borrow::Cow;
    use std::str::FromStr;

    /// Helper to build a test bundle
    fn build_test_bundle(source: &Eid, destination: &Eid, payload: &[u8]) -> Box<[u8]> {
        let builder = Builder::new(source.clone(), destination.clone())
            .with_lifetime(std::time::Duration::from_secs(3600))
            .with_payload(Cow::Borrowed(payload));
        let (_bundle, data) = builder.build(CreationTimestamp::now()).unwrap();
        data
    }

    #[test]
    fn extract_payload_round_trip() {
        let source = Eid::from_str("ipn:99.1").unwrap();
        let destination = Eid::from_str("ipn:2.1").unwrap();
        let payload = b"hello world";

        let bundle_bytes = build_test_bundle(&source, &destination, payload);

        let received = extract_payload(&bundle_bytes).unwrap();
        assert_eq!(received.payload, payload);
        assert_eq!(received.source, source);
    }

    #[test]
    fn extract_payload_empty_payload() {
        let source = Eid::from_str("ipn:1.0").unwrap();
        let destination = Eid::from_str("ipn:2.0").unwrap();
        let payload = b"";

        let bundle_bytes = build_test_bundle(&source, &destination, payload);

        let received = extract_payload(&bundle_bytes).unwrap();
        assert_eq!(received.payload, b"");
        assert_eq!(received.source, source);
    }

    #[test]
    fn extract_payload_invalid_bytes() {
        let result = extract_payload(b"not a valid bundle");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid BPv7 bundle"));
    }

    #[test]
    fn extract_payload_empty_bytes() {
        let result = extract_payload(b"");
        assert!(result.is_err());
    }

    #[test]
    fn output_filename_sanitizes_colons_and_dots() {
        let source = Eid::from_str("ipn:2.1").unwrap();
        let timestamp = CreationTimestamp::from_parts(Some(DtnTime::new(1234567890123)), 0);
        let filename = output_filename(&source, &timestamp);
        assert_eq!(filename, "ipn_2_1_1234567890123_0");
    }

    #[test]
    fn output_filename_includes_sequence_number() {
        let source = Eid::from_str("ipn:2.1").unwrap();
        let timestamp = CreationTimestamp::from_parts(Some(DtnTime::new(1000)), 42);
        let filename = output_filename(&source, &timestamp);
        assert_eq!(filename, "ipn_2_1_1000_42");
    }

    #[test]
    fn output_filename_handles_no_clock() {
        let source = Eid::from_str("ipn:5.3").unwrap();
        let timestamp = CreationTimestamp::from_parts(None, 7);
        let filename = output_filename(&source, &timestamp);
        assert_eq!(filename, "ipn_5_3_0_7");
    }

    #[test]
    fn output_filename_handles_null_eid() {
        let source = Eid::from_str("dtn:none").unwrap();
        let timestamp = CreationTimestamp::from_parts(Some(DtnTime::new(999)), 1);
        let filename = output_filename(&source, &timestamp);
        assert_eq!(filename, "dtn_none_999_1");
    }

    #[test]
    fn output_filename_different_inputs_produce_different_names() {
        let source = Eid::from_str("ipn:2.1").unwrap();
        let ts1 = CreationTimestamp::from_parts(Some(DtnTime::new(1000)), 0);
        let ts2 = CreationTimestamp::from_parts(Some(DtnTime::new(1001)), 0);
        assert_ne!(output_filename(&source, &ts1), output_filename(&source, &ts2));
    }

    #[test]
    fn write_payload_to_directory() {
        let dir = tempfile::tempdir().unwrap();
        let source = Eid::from_str("ipn:10.1").unwrap();
        let timestamp = CreationTimestamp::from_parts(Some(DtnTime::new(5000)), 0);
        let payload = b"hello bundle";

        let result = write_payload(payload, &source, &timestamp, Some(dir.path()));
        assert!(result.is_ok());

        let path = result.unwrap().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), payload);
        assert_eq!(path.file_name().unwrap().to_str().unwrap(), "ipn_10_1_5000_0");
    }

    #[test]
    fn write_payload_to_stdout_returns_none() {
        let source = Eid::from_str("ipn:1.1").unwrap();
        let timestamp = CreationTimestamp::from_parts(Some(DtnTime::new(100)), 0);
        let payload = b"test";

        let result = write_payload(payload, &source, &timestamp, None);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }
}
