use hardy_bpv7::eid::Eid;

/// Construct a BPv7 bundle with the given parameters.
///
/// Returns the serialized bundle as CBOR bytes.
pub fn build_bundle(
    _source: &Eid,
    _destination: &Eid,
    _payload: &[u8],
    _lifetime_secs: u64,
    _no_fragment: bool,
) -> anyhow::Result<Box<[u8]>> {
    todo!()
}
