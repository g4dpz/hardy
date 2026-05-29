use super::*;
use cla::{LinkDownProperties, LinkStateNotifier, LinkUpProperties};
use futures::{FutureExt, select_biased};
use hardy_async::sync::RwLock;
use hardy_bpv7::{
    eid::{Eid, NodeId},
    status_report::ReasonCode,
};
use hardy_eid_patterns::EidPattern;

pub(crate) mod agent;

mod find;
mod route;

#[derive(Debug)]
pub enum FindResult {
    AdminEndpoint,
    Deliver(Arc<services::registry::Service>), // Deliver to local service
    Forward(u32),                              // Forward to peer
    Drop(Option<ReasonCode>),                  // Drop with reason code
}

type RouteTable = BTreeMap<u32, BTreeMap<EidPattern, BTreeSet<route::Entry>>>; // priority -> pattern -> set of entries

struct RibInner {
    routes: RouteTable,
    address_types: HashMap<cla::ClaAddressType, Arc<cla::registry::Cla>>,
}

pub struct Rib {
    inner: RwLock<RibInner>,
    // Routing agent tracking: spin::Mutex for O(1) HashMap operations
    agents: hardy_async::sync::spin::Mutex<HashMap<String, Arc<agent::Agent>>>,
    node_ids: Arc<node_ids::NodeIds>,
    // Fixed per-instance seed for deterministic ECMP peer selection.
    // Random across BPA instances (unpredictable), but consistent within
    // an instance so the same bundle always selects the same peer.
    ecmp_hash_state: foldhash::quality::RandomState,
    tasks: hardy_async::TaskPool,
    poll_waiting_notify: Arc<hardy_async::Notify>,
    store: Arc<storage::Store>,

    // The priority for services - default 1
    service_priority: u32,

    // Link state notifiers: engine_id → notifier (for TVR link event dispatch)
    // CLAs register here during on_register to receive link state events from routing agents.
    link_notifiers: hardy_async::sync::spin::Mutex<HashMap<u64, Arc<dyn LinkStateNotifier>>>,
}

pub(crate) struct RibBuilder {
    agents: Vec<(String, Arc<dyn routes::RoutingAgent>)>,

    // The priority for services - default 1
    service_priority: u32,
}

impl RibBuilder {
    pub fn new() -> Self {
        Self {
            agents: Vec::new(),
            service_priority: 1,
        }
    }

    pub fn insert(&mut self, name: String, agent: Arc<dyn routes::RoutingAgent>) {
        self.agents.push((name, agent));
    }

    pub fn service_priority(&mut self, priority: u32) {
        self.service_priority = priority;
    }

    pub async fn build(
        self,
        node_ids: Arc<node_ids::NodeIds>,
        store: Arc<storage::Store>,
    ) -> routes::Result<Arc<Rib>> {
        let rib = Arc::new(Rib::new(node_ids, store, self.service_priority));
        for (name, agent) in self.agents {
            rib.register_agent(name, agent).await?;
        }
        Ok(rib)
    }
}

impl Rib {
    const ADMIN_NAME: &str = "administrative endpoint";
    const FORWARDS_NAME: &str = "neighbours";
    const SERVICES_NAME: &str = "services";

    fn new(
        node_ids: Arc<node_ids::NodeIds>,
        store: Arc<storage::Store>,
        service_priority: u32,
    ) -> Self {
        let entry = route::Entry {
            source: Self::ADMIN_NAME.into(),
            action: route::Action::AdminEndpoint,
        };

        // Add admin endpoints
        let mut admin_endpoints = BTreeMap::new();
        if let Some(node_name) = &node_ids.dtn {
            // Add the Admin Endpoint dtn EID itself (exact match, not wildcard)
            let admin_eid: Eid = node_name.clone().into();
            admin_endpoints.insert(admin_eid.into(), [entry.clone()].into());
        }

        if let Some(node_number) = &node_ids.ipn {
            // Add the Admin Endpoint ipn EID itself (exact match, not wildcard)
            let admin_eid: Eid = (*node_number).into();
            admin_endpoints.insert(admin_eid.into(), [entry].into());
        }

        let mut routes = BTreeMap::new();
        routes.insert(0, admin_endpoints);

        Self {
            inner: RwLock::new(RibInner {
                routes,
                address_types: HashMap::new(),
            }),
            agents: Default::default(),
            node_ids,
            ecmp_hash_state: foldhash::quality::RandomState::default(),
            tasks: hardy_async::TaskPool::new(),
            poll_waiting_notify: Arc::new(hardy_async::Notify::new()),
            store,
            service_priority,
            link_notifiers: Default::default(),
        }
    }

    pub(crate) fn start(self: &Arc<Self>, dispatcher: Arc<dispatcher::Dispatcher>) {
        let cancel_token = self.tasks.cancel_token().clone();
        let rib = self.clone();
        hardy_async::spawn!(self.tasks, "poll_waiting_task", async move {
            loop {
                select_biased! {
                    _ = cancel_token.cancelled().fuse() => {
                        break;
                    }
                    _ = rib.poll_waiting_notify.notified().fuse() => {
                        dispatcher.poll_waiting(cancel_token.clone()).await;
                    },
                }
            }

            debug!("Poll waiting task complete");
        });

        // Signal initial poll to pick up any pre-existing Waiting bundles
        self.poll_waiting_notify.notify_one();
    }

    pub async fn shutdown(&self) {
        self.tasks.shutdown().await;
    }

    async fn notify_updated(&self) {
        self.poll_waiting_notify.notify_waiters();
    }

    pub fn add_address_type(
        &self,
        address_type: cla::ClaAddressType,
        cla: Arc<cla::registry::Cla>,
    ) {
        self.inner.write().address_types.insert(address_type, cla);
    }

    pub fn remove_address_type(&self, address_type: &cla::ClaAddressType) {
        self.inner.write().address_types.remove(address_type);
    }

    /// Register a [`LinkStateNotifier`] for a specific engine ID.
    ///
    /// CLAs call this during `on_register` to advertise their ability to receive
    /// link state events for their configured spans. When a routing agent calls
    /// `notify_link_up`/`notify_link_down` on its [`RoutingSink`](crate::routes::RoutingSink),
    /// the RIB dispatches the event to the registered notifier for that engine ID.
    pub fn register_link_notifier(
        &self,
        engine_id: u64,
        notifier: Arc<dyn LinkStateNotifier>,
    ) {
        self.link_notifiers.lock().insert(engine_id, notifier);
        debug!("Registered link state notifier for engine_id={engine_id}");
    }

    /// Unregister a [`LinkStateNotifier`] for a specific engine ID.
    ///
    /// Called during CLA unregistration to clean up link event subscriptions.
    pub fn unregister_link_notifier(&self, engine_id: u64) {
        self.link_notifiers.lock().remove(&engine_id);
        debug!("Unregistered link state notifier for engine_id={engine_id}");
    }

    /// Dispatch a link-up event to the registered notifier for the given engine ID.
    ///
    /// If no notifier is registered for the engine ID, the event is silently discarded.
    pub(crate) async fn notify_link_up(&self, engine_id: u64, properties: LinkUpProperties) {
        let notifier = self.link_notifiers.lock().get(&engine_id).cloned();
        if let Some(notifier) = notifier {
            notifier.on_link_up(engine_id, properties).await;
        } else {
            debug!("No link state notifier registered for engine_id={engine_id}, discarding link-up event");
        }
    }

    /// Dispatch a link-down event to the registered notifier for the given engine ID.
    ///
    /// If no notifier is registered for the engine ID, the event is silently discarded.
    pub(crate) async fn notify_link_down(&self, engine_id: u64, properties: LinkDownProperties) {
        let notifier = self.link_notifiers.lock().get(&engine_id).cloned();
        if let Some(notifier) = notifier {
            notifier.on_link_down(engine_id, properties).await;
        } else {
            debug!("No link state notifier registered for engine_id={engine_id}, discarding link-down event");
        }
    }
}
