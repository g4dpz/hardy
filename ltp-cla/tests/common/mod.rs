use bytes::Bytes;
use hardy_bpa::async_trait;
use hardy_bpa::cla::{ClaAddress, Sink};

/// A minimal mock Sink for testing (no-op implementations).
pub struct MockSink;

#[async_trait]
impl Sink for MockSink {
    async fn unregister(&self) {}
    async fn dispatch(
        &self,
        _bundle: Bytes,
        _bp_addr: Option<&hardy_bpv7::eid::NodeId>,
        _cla_addr: Option<&ClaAddress>,
    ) -> hardy_bpa::cla::Result<()> {
        Ok(())
    }
    async fn add_peer(
        &self,
        _addr: ClaAddress,
        _node_ids: &[hardy_bpv7::eid::NodeId],
    ) -> hardy_bpa::cla::Result<bool> {
        Ok(true)
    }
    async fn remove_peer(&self, _addr: &ClaAddress) -> hardy_bpa::cla::Result<bool> {
        Ok(true)
    }
}
