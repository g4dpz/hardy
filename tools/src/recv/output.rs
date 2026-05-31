use hardy_bpv7::eid::Eid;
use hardy_bpv7::creation_timestamp::CreationTimestamp;

/// Extract the payload from a received BPv7 bundle.
pub fn extract_payload(_bundle_bytes: &[u8]) -> anyhow::Result<(Eid, CreationTimestamp, Vec<u8>)> {
    todo!()
}

/// Generate a unique output filename from the bundle source EID and creation timestamp.
pub fn output_filename(_source: &Eid, _timestamp: &CreationTimestamp) -> String {
    todo!()
}
