use hardy_bpv7::{builder::Builder, bundle, creation_timestamp::CreationTimestamp, eid::Eid};
use std::borrow::Cow;

/// Construct a BPv7 bundle with the given parameters.
///
/// Returns the serialized bundle as CBOR bytes.
pub fn build_bundle(
    source: &Eid,
    destination: &Eid,
    payload: &[u8],
    lifetime_secs: u64,
    no_fragment: bool,
) -> anyhow::Result<Box<[u8]>> {
    let mut builder = Builder::new(source.clone(), destination.clone())
        .with_lifetime(std::time::Duration::from_secs(lifetime_secs))
        .with_payload(Cow::Borrowed(payload));

    if no_fragment {
        builder = builder.with_flags(bundle::Flags {
            do_not_fragment: true,
            ..Default::default()
        });
    }

    let (_bundle, data) = builder.build(CreationTimestamp::now())?;
    Ok(data)
}
