//! `plugin-sdk-rs` — Rust SDK for HomeCore device plugins.
//!
//! Provides:
//! - [`PluginClient`] — connects to the broker, handles registration, typed
//!   publish/subscribe helpers, and a command callback loop.
//! - [`DevicePublisher`] — cloneable handle for publishing state from spawned tasks.
//! - [`ManagementHandle`] — enable heartbeat + remote config/log management.

pub mod mqtt_log_layer;
pub mod streaming;

use anyhow::{Context, Result};
use hc_types::device::{change_from_command_payload, with_state_change_metadata, DeviceChange};
use rumqttc::{AsyncClient, EventLoop, MqttOptions, Packet, QoS};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, error, info, warn};

pub use streaming::{StreamContext, StreamingAction};

/// Shared tracker of MQTT topics this plugin has subscribed to.
/// On reconnect (ConnAck), all tracked topics are re-subscribed.
type SubscriptionTracker = Arc<Mutex<HashSet<String>>>;

/// No-op state callback used when the plugin doesn't need to observe other
/// devices (most plugins). Passed to `run_inner` from `run` / `run_managed`.
fn noop_state_cb(_device_id: String, _state: Value) {}

/// Inner state of the device tracker. Holds the set of device_ids this
/// plugin has registered with HomeCore plus an optional path to mirror
/// the set onto disk so it survives plugin restarts.
///
/// Persistence is opt-in via [`PluginClient::with_device_persistence`]
/// or [`DevicePublisher::enable_persistence`]. When enabled, every
/// register/unregister mutation is followed by a synchronous JSON
/// write — fine for the typical scale (dozens to a few hundred
/// devices) and avoids any reconnect/restart races.
#[derive(Default)]
pub(crate) struct DeviceTrackerInner {
    set: HashSet<String>,
    persist_path: Option<std::path::PathBuf>,
}

impl DeviceTrackerInner {
    fn enable_persistence(&mut self, path: std::path::PathBuf) {
        if let Ok(body) = std::fs::read_to_string(&path) {
            if let Ok(ids) = serde_json::from_str::<Vec<String>>(&body) {
                self.set.extend(ids);
            }
        }
        self.persist_path = Some(path);
    }

    fn insert(&mut self, id: &str) {
        if self.set.insert(id.to_string()) {
            self.save();
        }
    }

    fn remove(&mut self, id: &str) {
        if self.set.remove(id) {
            self.save();
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.set.len()
    }

    fn snapshot(&self) -> HashSet<String> {
        self.set.clone()
    }

    fn save(&self) {
        let Some(p) = &self.persist_path else { return };
        if let Some(parent) = p.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let mut sorted: Vec<&String> = self.set.iter().collect();
        sorted.sort();
        if let Ok(body) = serde_json::to_vec_pretty(&sorted) {
            if let Err(e) = std::fs::write(p, body) {
                warn!(path = %p.display(), error = %e, "device tracker persistence write failed");
            }
        }
    }
}

/// Shared tracker of device IDs this plugin has registered with HomeCore.
/// Shared between `PluginClient` and `DevicePublisher` so registrations
/// from spawned tasks are reflected in the heartbeat's `device_count`
/// and in `reconcile_devices`.
type DeviceTracker = Arc<Mutex<DeviceTrackerInner>>;

/// Outcome of [`DevicePublisher::reconcile_devices`].
#[derive(Debug, Default)]
pub struct ReconcileReport {
    /// Device IDs that were registered before this reconcile but are
    /// not in the supplied `live` set, so were unregistered.
    pub stale_unregistered: Vec<String>,
    /// Device IDs that were in the `live` set but had not been
    /// registered. Usually empty — non-empty means the caller passed
    /// a live set that includes devices it never registered with the
    /// SDK. Logged for diagnostic value, no action taken.
    pub unknown_in_live: Vec<String>,
}

/// A cloneable handle for publishing device state from outside the `run()` loop.
///
/// Obtained via [`PluginClient::device_publisher`] before calling `run()`.
#[derive(Clone)]
pub struct DevicePublisher {
    client: AsyncClient,
    plugin_id: String,
    subscriptions: SubscriptionTracker,
    devices: DeviceTracker,
}

pub fn change_from_command(command_payload: &Value, fallback_source: &str) -> DeviceChange {
    change_from_command_payload(command_payload, fallback_source)
}

impl DevicePublisher {
    /// Return the plugin ID this publisher was created with.
    pub fn plugin_id(&self) -> &str {
        &self.plugin_id
    }

    async fn clear_retained_topic(&self, topic: &str) -> Result<()> {
        self.client
            .publish(topic, QoS::AtLeastOnce, true, Vec::<u8>::new())
            .await
            .with_context(|| format!("clear retained topic failed: {topic}"))
    }

    // ── Full state publishing ────────────────────────────────────────────

    /// Publish a full device state to `homecore/devices/{device_id}/state` (retained).
    pub async fn publish_state(&self, device_id: &str, state: &Value) -> Result<()> {
        self.publish_state_with_change(device_id, state, None).await
    }

    /// Publish a full device state with explicit provenance metadata.
    pub async fn publish_state_with_change(
        &self,
        device_id: &str,
        state: &Value,
        change: Option<&DeviceChange>,
    ) -> Result<()> {
        let topic = format!("homecore/devices/{device_id}/state");
        let payload = match change {
            Some(change) => serde_json::to_vec(&with_state_change_metadata(state.clone(), change))?,
            None => serde_json::to_vec(state)?,
        };
        self.client
            .publish(&topic, QoS::AtLeastOnce, true, payload)
            .await
            .context("publish_state failed")
    }

    /// Publish a full device state caused by an inbound HomeCore command.
    pub async fn publish_state_for_command(
        &self,
        device_id: &str,
        state: &Value,
        command_payload: &Value,
        fallback_source: &str,
    ) -> Result<()> {
        let change = change_from_command(command_payload, fallback_source);
        self.publish_state_with_change(device_id, state, Some(&change))
            .await
    }

    // ── Partial state publishing ─────────────────────────────────────────

    /// Publish a partial state update (JSON merge-patch, not retained).
    pub async fn publish_state_partial(&self, device_id: &str, patch: &Value) -> Result<()> {
        self.publish_state_partial_with_change(device_id, patch, None)
            .await
    }

    /// Publish a partial state update with explicit provenance metadata.
    pub async fn publish_state_partial_with_change(
        &self,
        device_id: &str,
        patch: &Value,
        change: Option<&DeviceChange>,
    ) -> Result<()> {
        let topic = format!("homecore/devices/{device_id}/state/partial");
        let payload = match change {
            Some(change) => serde_json::to_vec(&with_state_change_metadata(patch.clone(), change))?,
            None => serde_json::to_vec(patch)?,
        };
        self.client
            .publish(&topic, QoS::AtLeastOnce, false, payload)
            .await
            .context("publish_state_partial failed")
    }

    /// Publish a partial state update caused by an inbound HomeCore command.
    pub async fn publish_state_partial_for_command(
        &self,
        device_id: &str,
        patch: &Value,
        command_payload: &Value,
        fallback_source: &str,
    ) -> Result<()> {
        let change = change_from_command(command_payload, fallback_source);
        self.publish_state_partial_with_change(device_id, patch, Some(&change))
            .await
    }

    // ── Availability ─────────────────────────────────────────────────────

    /// Publish `"online"` or `"offline"` to the device's availability topic (retained).
    pub async fn set_available(&self, device_id: &str, available: bool) -> Result<()> {
        let topic = format!("homecore/devices/{device_id}/availability");
        let payload = if available { "online" } else { "offline" };
        self.client
            .publish(&topic, QoS::AtLeastOnce, true, payload.as_bytes())
            .await
            .context("set_available failed")
    }

    /// Alias for [`set_available`] — matches the naming used by most plugins.
    pub async fn publish_availability(&self, device_id: &str, online: bool) -> Result<()> {
        self.set_available(device_id, online).await
    }

    // ── Schema ───────────────────────────────────────────────────────────

    /// Publish a device capability schema (retained) so HomeCore stores it and
    /// API clients can retrieve it via `GET /api/v1/devices/{id}/schema`.
    pub async fn register_device_schema(
        &self,
        device_id: &str,
        schema: &hc_types::DeviceSchema,
    ) -> Result<()> {
        let topic = format!("homecore/devices/{device_id}/schema");
        let payload = serde_json::to_vec(schema).context("serialising device schema")?;
        self.client
            .publish(&topic, QoS::AtLeastOnce, true, payload)
            .await
            .context("register_device_schema failed")
    }

    // ── Unregister ───────────────────────────────────────────────────────

    /// Retire a device from HomeCore by clearing retained topics and publishing
    /// a plugin-scoped unregister command.
    pub async fn unregister_device(&self, plugin_id: &str, device_id: &str) -> Result<()> {
        self.clear_retained_topic(&format!("homecore/devices/{device_id}/state"))
            .await?;
        self.clear_retained_topic(&format!("homecore/devices/{device_id}/availability"))
            .await?;
        self.clear_retained_topic(&format!("homecore/devices/{device_id}/schema"))
            .await?;
        self.client
            .publish(
                format!("homecore/plugins/{plugin_id}/unregister"),
                QoS::AtLeastOnce,
                false,
                serde_json::to_vec(&serde_json::json!({ "device_id": device_id }))?,
            )
            .await
            .context("unregister_device failed")?;
        self.devices.lock().unwrap().remove(device_id);
        Ok(())
    }

    // ── Plugin status ────────────────────────────────────────────────────

    /// Publish plugin status (`"active"`, `"degraded"`, `"offline"`) to
    /// `homecore/plugins/{id}/status` (retained).
    pub async fn publish_plugin_status(&self, status: &str) -> Result<()> {
        let topic = format!("homecore/plugins/{}/status", self.plugin_id);
        self.client
            .publish(&topic, QoS::AtLeastOnce, true, status.as_bytes())
            .await
            .context("publish_plugin_status failed")
    }

    // ── Events ───────────────────────────────────────────────────────────

    /// Publish a structured event to `homecore/events/{event_type}`.
    pub async fn publish_event(&self, event_type: &str, payload: &Value) -> Result<()> {
        let topic = format!("homecore/events/{event_type}");
        self.client
            .publish(
                &topic,
                QoS::AtLeastOnce,
                false,
                serde_json::to_vec(payload)?,
            )
            .await
            .context("publish_event failed")
    }

    // ── Dynamic registration (for plugins that discover devices at runtime) ─

    /// Register a device with all optional fields via the publisher.
    ///
    /// This mirrors [`PluginClient::register_device_full`] but can be called
    /// from spawned tasks that only hold a `DevicePublisher` handle (after the
    /// `PluginClient` has been consumed by `run_managed`).
    pub async fn register_device_full(
        &self,
        device_id: &str,
        name: &str,
        device_type: Option<&str>,
        area: Option<&str>,
        capabilities: Option<Value>,
    ) -> Result<()> {
        let topic = format!("homecore/plugins/{}/register", self.plugin_id);
        let mut payload = serde_json::json!({
            "device_id": device_id,
            "plugin_id": self.plugin_id,
            "name": name,
        });
        if let Some(dt) = device_type {
            payload["device_type"] = Value::String(dt.to_string());
        }
        if let Some(a) = area {
            payload["area"] = Value::String(a.to_string());
        }
        if let Some(c) = capabilities {
            payload["capabilities"] = c;
        }
        self.client
            .publish(
                &topic,
                QoS::AtLeastOnce,
                false,
                serde_json::to_vec(&payload)?,
            )
            .await
            .context("DevicePublisher::register_device_full failed")?;
        self.devices.lock().unwrap().insert(device_id);
        Ok(())
    }

    /// Enable cross-restart persistence for the device tracker.
    ///
    /// Loads any previously-saved device IDs from `path` into the
    /// in-memory tracker, then mirrors every register/unregister
    /// mutation back to disk. Combined with [`Self::reconcile_devices`]
    /// this gives plugins a "set what's live this cycle, SDK cleans
    /// up everything else" workflow that survives plugin restarts.
    ///
    /// Idempotent — call once at startup, before the first
    /// `register_device_full` of the session. Multiple calls re-load
    /// from the same path which is harmless but pointless.
    ///
    /// Path is typically `<config_dir>/.published-device-ids.json`.
    pub fn enable_persistence(&self, path: std::path::PathBuf) {
        self.devices.lock().unwrap().enable_persistence(path);
    }

    /// Reconcile the live device set against everything this plugin
    /// has ever registered (both in-session via `register_device_full`
    /// and across restarts when persistence is enabled).
    ///
    /// `live` should be the set of device_ids the plugin's authoritative
    /// upstream (Hue bridge, Z-Wave network, YoLink cloud, etc.)
    /// reports as currently existing. Anything previously registered
    /// but absent from `live` gets `unregister_device`d, removed from
    /// the tracker, and (if persistence is on) the new live set is
    /// written to disk.
    ///
    /// Caller is responsible for not invoking this when an upstream
    /// fetch failed — a partial live set would otherwise wipe legit
    /// devices behind a temporarily-unreachable upstream. Typical
    /// pattern: track an `all_bridges_succeeded` flag, only call
    /// `reconcile_devices` when true.
    ///
    /// New devices in `live` that the plugin hasn't yet registered
    /// are reported in `unknown_in_live` but otherwise ignored — call
    /// `register_device_full` for them first to bring them into the
    /// tracker.
    pub async fn reconcile_devices(
        &self,
        live: HashSet<String>,
    ) -> Result<ReconcileReport> {
        let known = self.devices.lock().unwrap().snapshot();
        let stale: Vec<String> = known.difference(&live).cloned().collect();
        let unknown_in_live: Vec<String> = live.difference(&known).cloned().collect();
        let mut unregistered = Vec::with_capacity(stale.len());
        for id in &stale {
            match self.unregister_device(&self.plugin_id, id).await {
                Ok(()) => {
                    unregistered.push(id.clone());
                    info!(plugin_id = %self.plugin_id, device_id = %id, "Unregistered stale device");
                }
                Err(e) => {
                    warn!(plugin_id = %self.plugin_id, device_id = %id, error = %e, "Failed to unregister stale device");
                }
            }
        }
        if !unknown_in_live.is_empty() {
            debug!(
                plugin_id = %self.plugin_id,
                count = unknown_in_live.len(),
                "reconcile_devices saw live ids not yet registered with the SDK; \
                 caller should `register_device_full` first"
            );
        }
        Ok(ReconcileReport {
            stale_unregistered: unregistered,
            unknown_in_live,
        })
    }

    /// Subscribe to command messages for a device and track the subscription
    /// so it is restored on MQTT reconnect.
    ///
    /// This mirrors [`PluginClient::subscribe_commands`] but can be called
    /// from spawned tasks that only hold a `DevicePublisher` handle.
    pub async fn subscribe_commands(&self, device_id: &str) -> Result<()> {
        let topic = format!("homecore/devices/{device_id}/cmd");
        self.client
            .subscribe(&topic, QoS::AtLeastOnce)
            .await
            .context("DevicePublisher::subscribe_commands failed")?;
        self.subscriptions.lock().unwrap().insert(topic);
        Ok(())
    }

    /// Subscribe to state updates for a device this plugin does **not** own.
    /// Tracked in the shared subscription set so reconnect restores it.
    ///
    /// Mirrors [`PluginClient::subscribe_state`].
    pub async fn subscribe_state(&self, device_id: &str) -> Result<()> {
        let topic_full = format!("homecore/devices/{device_id}/state");
        let topic_partial = format!("homecore/devices/{device_id}/state/partial");
        self.client
            .subscribe(&topic_full, QoS::AtLeastOnce)
            .await
            .context("DevicePublisher::subscribe_state (full) failed")?;
        self.client
            .subscribe(&topic_partial, QoS::AtLeastOnce)
            .await
            .context("DevicePublisher::subscribe_state (partial) failed")?;
        let mut subs = self.subscriptions.lock().unwrap();
        subs.insert(topic_full);
        subs.insert(topic_partial);
        Ok(())
    }

    /// Remove a subscription previously added with [`subscribe_state`].
    pub async fn unsubscribe_state(&self, device_id: &str) -> Result<()> {
        let topic_full = format!("homecore/devices/{device_id}/state");
        let topic_partial = format!("homecore/devices/{device_id}/state/partial");
        let _ = self.client.unsubscribe(&topic_full).await;
        let _ = self.client.unsubscribe(&topic_partial).await;
        let mut subs = self.subscriptions.lock().unwrap();
        subs.remove(&topic_full);
        subs.remove(&topic_partial);
        Ok(())
    }

    /// Create a `DevicePublisher` for use in unit tests.
    ///
    /// The underlying MQTT client is connected to `127.0.0.1:1883` and will
    /// not actually send messages unless a broker is running.
    pub fn test_instance(plugin_id: &str) -> Self {
        use rumqttc::MqttOptions;
        use std::time::Duration;
        let mut opts = MqttOptions::new(format!("{plugin_id}-test"), "127.0.0.1", 1883);
        opts.set_keep_alive(Duration::from_secs(30));
        let (client, _eventloop) = AsyncClient::new(opts, 8);
        Self {
            client,
            plugin_id: plugin_id.to_string(),
            subscriptions: Arc::new(Mutex::new(HashSet::new())),
            devices: Arc::new(Mutex::new(DeviceTrackerInner::default())),
        }
    }
}

/// Handle returned by [`PluginClient::enable_management`].
///
/// Pass this to [`PluginClient::run_managed`] to automatically handle
/// `get_config`, `set_config`, and `set_log_level` management commands.
#[derive(Clone)]
pub struct ManagementHandle {
    plugin_id: String,
    config_path: Option<String>,
    log_level_handle: Option<hc_logging::LogLevelHandle>,
    custom_handler: Option<Arc<dyn Fn(&Value) -> Option<Value> + Send + Sync>>,
    /// Capability manifest, published retained on
    /// `homecore/plugins/{id}/capabilities` after the first CONNACK.
    capabilities: Option<hc_types::Capabilities>,
    /// Registered streaming action handlers, indexed by action id.
    streaming_actions: Arc<HashMap<String, StreamingAction>>,
    /// Live streams, keyed by `request_id`. Entries are added on
    /// dispatch and removed after the action closure exits.
    active_streams: streaming::ActiveStreams,
}

impl ManagementHandle {
    /// Install a plugin-specific handler for actions not recognised by the
    /// built-in dispatcher (`ping`, `get_config`, `set_config`, `set_log_level`).
    ///
    /// Return `Some(response)` to handle the action (the SDK fills in
    /// `request_id` automatically), or `None` to fall through to the standard
    /// "unknown action" error.
    pub fn with_custom_handler<F>(mut self, f: F) -> Self
    where
        F: Fn(&Value) -> Option<Value> + Send + Sync + 'static,
    {
        self.custom_handler = Some(Arc::new(f));
        self
    }

    /// Declare the plugin's capability manifest. The SDK publishes it
    /// retained on `homecore/plugins/{id}/capabilities` after each CONNACK
    /// so reconnects refresh the cached manifest.
    ///
    /// `spec` and `plugin_id` are set by the SDK if empty — callers only
    /// need to provide the `actions` list in most cases.
    pub fn with_capabilities(mut self, mut caps: hc_types::Capabilities) -> Self {
        if caps.spec.is_empty() {
            caps.spec = "1".into();
        }
        if caps.plugin_id.is_empty() {
            caps.plugin_id = self.plugin_id.clone();
        }
        self.capabilities = Some(caps);
        self
    }

    /// Register a handler for a streaming action declared in the
    /// capability manifest. When `homecore/plugins/{id}/manage/cmd`
    /// receives a command whose `action` matches `action.id()`, the SDK
    /// replies `status:"accepted"` and spawns the closure with a fresh
    /// [`StreamContext`]. The closure must emit exactly one terminal
    /// stage before returning.
    pub fn with_streaming_action(mut self, action: StreamingAction) -> Self {
        // Arc<HashMap<_,_>> is immutable after clone; rebuild on add.
        let mut map: HashMap<String, StreamingAction> =
            (*self.streaming_actions).clone();
        map.insert(action.id.clone(), action);
        self.streaming_actions = Arc::new(map);
        self
    }
}

/// Connection configuration for a plugin.
#[derive(Debug, Clone)]
pub struct PluginConfig {
    pub broker_host: String,
    pub broker_port: u16,
    pub plugin_id: String,
    pub password: String,
}

impl Default for PluginConfig {
    fn default() -> Self {
        Self {
            broker_host: "127.0.0.1".into(),
            broker_port: 1883,
            plugin_id: "plugin.unnamed".into(),
            password: String::new(),
        }
    }
}

/// Callback type invoked when a command arrives for a device.
pub type CommandHandler = Box<dyn Fn(String, Value) + Send + Sync + 'static>;

/// A connected plugin client.
pub struct PluginClient {
    client: AsyncClient,
    eventloop: EventLoop,
    config: PluginConfig,
    subscriptions: SubscriptionTracker,
    devices: DeviceTracker,
}

impl PluginClient {
    async fn clear_retained_topic(&self, topic: &str) -> Result<()> {
        self.client
            .publish(topic, QoS::AtLeastOnce, true, Vec::<u8>::new())
            .await
            .with_context(|| format!("clear retained topic failed: {topic}"))
    }

    /// Connect to the HomeCore broker and return a ready client.
    pub async fn connect(config: PluginConfig) -> Result<Self> {
        let mut opts = MqttOptions::new(&config.plugin_id, &config.broker_host, config.broker_port);
        opts.set_keep_alive(Duration::from_secs(30));
        opts.set_clean_session(true);
        if !config.password.is_empty() {
            opts.set_credentials(&config.plugin_id, &config.password);
        }

        let (client, eventloop) = AsyncClient::new(opts, 64);
        info!(plugin_id = %config.plugin_id, "Plugin connecting");
        Ok(Self {
            client,
            eventloop,
            config,
            subscriptions: Arc::new(Mutex::new(HashSet::new())),
            devices: Arc::new(Mutex::new(DeviceTrackerInner::default())),
        })
    }

    /// Return the plugin ID.
    pub fn plugin_id(&self) -> &str {
        &self.config.plugin_id
    }

    /// Return a clone of the underlying MQTT client handle.
    pub fn mqtt_client(&self) -> AsyncClient {
        self.client.clone()
    }

    /// Enable cross-restart persistence for the device tracker.
    ///
    /// Builder-style — call once after `connect`, before any
    /// `register_device_full` calls. Loads any previously-saved
    /// device IDs into the in-memory tracker so
    /// [`DevicePublisher::reconcile_devices`] can clean up devices
    /// that disappeared while the plugin was offline.
    ///
    /// Path is typically `<config_dir>/.published-device-ids.json`.
    pub fn with_device_persistence(self, path: std::path::PathBuf) -> Self {
        self.devices.lock().unwrap().enable_persistence(path);
        self
    }

    // ── Full state publishing ────────────────────────────────────────────

    /// Publish a full device state update (retained so new subscribers see it).
    pub async fn publish_state(&self, device_id: &str, state: &Value) -> Result<()> {
        self.publish_state_with_change(device_id, state, None).await
    }

    /// Publish a full device state update with explicit provenance metadata.
    pub async fn publish_state_with_change(
        &self,
        device_id: &str,
        state: &Value,
        change: Option<&DeviceChange>,
    ) -> Result<()> {
        let topic = format!("homecore/devices/{device_id}/state");
        let payload = match change {
            Some(change) => serde_json::to_vec(&with_state_change_metadata(state.clone(), change))?,
            None => serde_json::to_vec(state)?,
        };
        self.client
            .publish(&topic, QoS::AtLeastOnce, true, payload)
            .await
            .context("publish_state failed")
    }

    /// Publish a full device state caused by an inbound HomeCore command.
    pub async fn publish_state_for_command(
        &self,
        device_id: &str,
        state: &Value,
        command_payload: &Value,
        fallback_source: &str,
    ) -> Result<()> {
        let change = change_from_command(command_payload, fallback_source);
        self.publish_state_with_change(device_id, state, Some(&change))
            .await
    }

    // ── Partial state publishing ─────────────────────────────────────────

    /// Publish a partial state update (JSON merge-patch, not retained).
    pub async fn publish_state_partial(&self, device_id: &str, patch: &Value) -> Result<()> {
        self.publish_state_partial_with_change(device_id, patch, None)
            .await
    }

    /// Publish a partial state update with explicit provenance metadata.
    pub async fn publish_state_partial_with_change(
        &self,
        device_id: &str,
        patch: &Value,
        change: Option<&DeviceChange>,
    ) -> Result<()> {
        let topic = format!("homecore/devices/{device_id}/state/partial");
        let payload = match change {
            Some(change) => serde_json::to_vec(&with_state_change_metadata(patch.clone(), change))?,
            None => serde_json::to_vec(patch)?,
        };
        self.client
            .publish(&topic, QoS::AtLeastOnce, false, payload)
            .await
            .context("publish_state_partial failed")
    }

    /// Publish a partial state update caused by an inbound HomeCore command.
    pub async fn publish_state_partial_for_command(
        &self,
        device_id: &str,
        patch: &Value,
        command_payload: &Value,
        fallback_source: &str,
    ) -> Result<()> {
        let change = change_from_command(command_payload, fallback_source);
        self.publish_state_partial_with_change(device_id, patch, Some(&change))
            .await
    }

    // ── Availability ─────────────────────────────────────────────────────

    /// Publish `"online"` or `"offline"` to the device's availability topic.
    pub async fn set_available(&self, device_id: &str, available: bool) -> Result<()> {
        let topic = format!("homecore/devices/{device_id}/availability");
        let payload = if available { "online" } else { "offline" };
        self.client
            .publish(&topic, QoS::AtLeastOnce, true, payload.as_bytes())
            .await
            .context("set_available failed")
    }

    /// Alias for [`set_available`] — matches the naming used by most plugins.
    pub async fn publish_availability(&self, device_id: &str, online: bool) -> Result<()> {
        self.set_available(device_id, online).await
    }

    // ── Device registration ──────────────────────────────────────────────

    /// Register a device with its capability schema.
    pub async fn register_device(
        &self,
        device_id: &str,
        name: &str,
        capabilities: Value,
    ) -> Result<()> {
        let topic = format!("homecore/plugins/{}/register", self.config.plugin_id);
        let payload = serde_json::json!({
            "device_id": device_id,
            "plugin_id": self.config.plugin_id,
            "name": name,
            "capabilities": capabilities,
        });
        self.client
            .publish(
                &topic,
                QoS::AtLeastOnce,
                false,
                serde_json::to_vec(&payload)?,
            )
            .await
            .context("register_device failed")?;
        self.devices.lock().unwrap().insert(device_id);
        info!(device_id, "Device registered");
        Ok(())
    }

    /// Register a device by type name.
    ///
    /// Instead of providing a full capability schema, supply a `device_type` string
    /// that HomeCore resolves against its built-in device-type catalog (loaded from
    /// `config/profiles/examples/device-types.toml`).  This is the recommended
    /// registration path for well-known device categories.
    ///
    /// # Example types
    /// `"light"`, `"switch"`, `"motion_sensor"`, `"contact_sensor"`,
    /// `"temperature_sensor"`, `"power_monitor"`, `"cover"`, `"lock"`,
    /// `"climate"`, `"virtual_switch"`, …
    pub async fn register_device_typed(
        &self,
        device_id: &str,
        name: &str,
        device_type: &str,
        area: Option<&str>,
    ) -> Result<()> {
        self.register_device_full(device_id, name, Some(device_type), area, None)
            .await
    }

    /// Register a device with all optional fields: device_type, area, and capabilities.
    ///
    /// This is the most flexible registration method. Use it when you need to
    /// combine a device_type with custom capabilities, or when you need to set
    /// the area alongside capabilities.
    pub async fn register_device_full(
        &self,
        device_id: &str,
        name: &str,
        device_type: Option<&str>,
        area: Option<&str>,
        capabilities: Option<Value>,
    ) -> Result<()> {
        let topic = format!("homecore/plugins/{}/register", self.config.plugin_id);
        let mut payload = serde_json::json!({
            "device_id":   device_id,
            "plugin_id":   self.config.plugin_id,
            "name":        name,
        });
        if let Some(dt) = device_type {
            payload["device_type"] = serde_json::Value::String(dt.to_string());
        }
        if let Some(a) = area {
            payload["area"] = serde_json::Value::String(a.to_string());
        }
        if let Some(c) = capabilities {
            payload["capabilities"] = c;
        }
        self.client
            .publish(
                &topic,
                QoS::AtLeastOnce,
                false,
                serde_json::to_vec(&payload)?,
            )
            .await
            .context("register_device_full failed")?;
        self.devices.lock().unwrap().insert(device_id);
        info!(device_id, "Device registered");
        Ok(())
    }

    // ── Schema ───────────────────────────────────────────────────────────

    /// Publish a device capability schema (retained) so HomeCore stores it and
    /// API clients can retrieve it via `GET /api/v1/devices/{id}/schema`.
    pub async fn register_device_schema(
        &self,
        device_id: &str,
        schema: &hc_types::DeviceSchema,
    ) -> Result<()> {
        let topic = format!("homecore/devices/{device_id}/schema");
        let payload = serde_json::to_vec(schema).context("serialising device schema")?;
        self.client
            .publish(&topic, QoS::AtLeastOnce, true, payload)
            .await
            .context("register_device_schema failed")
    }

    // ── Unregister ───────────────────────────────────────────────────────

    /// Retire a device from HomeCore by clearing retained topics and publishing
    /// a plugin-scoped unregister command.
    pub async fn unregister_device(&self, device_id: &str) -> Result<()> {
        self.clear_retained_topic(&format!("homecore/devices/{device_id}/state"))
            .await?;
        self.clear_retained_topic(&format!("homecore/devices/{device_id}/availability"))
            .await?;
        self.clear_retained_topic(&format!("homecore/devices/{device_id}/schema"))
            .await?;
        self.client
            .publish(
                format!("homecore/plugins/{}/unregister", self.config.plugin_id),
                QoS::AtLeastOnce,
                false,
                serde_json::to_vec(&serde_json::json!({ "device_id": device_id }))?,
            )
            .await
            .context("unregister_device failed")?;
        self.devices.lock().unwrap().remove(device_id);
        info!(device_id, "Device unregistered");
        Ok(())
    }

    // ── Plugin status ────────────────────────────────────────────────────

    /// Publish plugin status (`"active"`, `"degraded"`, `"offline"`) to
    /// `homecore/plugins/{id}/status` (retained).
    pub async fn publish_plugin_status(&self, status: &str) -> Result<()> {
        let topic = format!("homecore/plugins/{}/status", self.config.plugin_id);
        self.client
            .publish(&topic, QoS::AtLeastOnce, true, status.as_bytes())
            .await
            .context("publish_plugin_status failed")
    }

    // ── Events ───────────────────────────────────────────────────────────

    /// Publish a structured event to `homecore/events/{event_type}`.
    pub async fn publish_event(&self, event_type: &str, payload: &Value) -> Result<()> {
        let topic = format!("homecore/events/{event_type}");
        self.client
            .publish(
                &topic,
                QoS::AtLeastOnce,
                false,
                serde_json::to_vec(payload)?,
            )
            .await
            .context("publish_event failed")
    }

    // ── Publisher handle ─────────────────────────────────────────────────

    /// Return a [`DevicePublisher`] that can publish state concurrently with `run()`.
    ///
    /// Call this **before** `run()` — `run()` consumes `self`, so any handles
    /// must be obtained first.  The returned publisher is `Clone`.
    pub fn device_publisher(&self) -> DevicePublisher {
        DevicePublisher {
            client: self.client.clone(),
            plugin_id: self.config.plugin_id.clone(),
            subscriptions: Arc::clone(&self.subscriptions),
            devices: Arc::clone(&self.devices),
        }
    }

    // ── Management ───────────────────────────────────────────────────────

    /// Enable the management protocol: heartbeat publisher + command listener.
    ///
    /// Call this **before** `run()`.  The heartbeat is published every
    /// `interval_secs` seconds to `homecore/plugins/{id}/heartbeat`.
    /// Management commands arrive on `homecore/plugins/{id}/manage/cmd` and are
    /// dispatched inside `run()` via the provided callbacks.
    ///
    /// `config_path` is the plugin's config file path — used to implement
    /// `get_config` and `set_config` commands automatically.
    pub async fn enable_management(
        &self,
        interval_secs: u64,
        version: Option<String>,
        config_path: Option<String>,
        log_level_handle: Option<hc_logging::LogLevelHandle>,
    ) -> Result<ManagementHandle> {
        // Track management subscription for reconnect.
        let mgmt_topic = format!("homecore/plugins/{}/manage/cmd", self.config.plugin_id);
        self.client
            .subscribe(&mgmt_topic, QoS::AtLeastOnce)
            .await
            .context("subscribe management/cmd failed")?;
        self.subscriptions.lock().unwrap().insert(mgmt_topic);

        // Spawn heartbeat publisher.
        let hb_client = self.client.clone();
        let hb_plugin_id = self.config.plugin_id.clone();
        let hb_version = version.clone();
        let hb_devices = Arc::clone(&self.devices);
        let started_at = std::time::Instant::now();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
            loop {
                interval.tick().await;
                let uptime_secs = started_at.elapsed().as_secs();
                let device_count = hb_devices.lock().unwrap().len() as u64;
                let payload = serde_json::json!({
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                    "version": hb_version,
                    "uptime_secs": uptime_secs,
                    "device_count": device_count,
                });
                let topic = format!("homecore/plugins/{hb_plugin_id}/heartbeat");
                let _ = hb_client
                    .publish(
                        &topic,
                        QoS::AtMostOnce,
                        false,
                        serde_json::to_vec(&payload).unwrap_or_default(),
                    )
                    .await;
            }
        });

        info!(plugin_id = %self.config.plugin_id, "Management protocol enabled (heartbeat every {interval_secs}s)");
        Ok(ManagementHandle {
            plugin_id: self.config.plugin_id.clone(),
            config_path,
            log_level_handle,
            custom_handler: None,
            capabilities: None,
            streaming_actions: Arc::new(HashMap::new()),
            active_streams: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    // ── Command subscriptions ────────────────────────────────────────────

    /// Subscribe to command messages for a device.
    ///
    /// The subscription is tracked and automatically restored on MQTT reconnect
    /// (clean_session=true loses subscriptions on disconnect).
    pub async fn subscribe_commands(&self, device_id: &str) -> Result<()> {
        let topic = format!("homecore/devices/{device_id}/cmd");
        self.client
            .subscribe(&topic, QoS::AtLeastOnce)
            .await
            .context("subscribe_commands failed")?;
        self.subscriptions.lock().unwrap().insert(topic);
        debug!(device_id, "Subscribed to commands");
        Ok(())
    }

    // ── External state subscription (cross-device consumer plugins) ─────

    /// Subscribe to state updates for a device this plugin does **not** own.
    ///
    /// Used by cross-device consumer plugins (e.g. thermostats observing
    /// external temperature sensors). The subscription is tracked and
    /// automatically restored on MQTT reconnect, just like
    /// [`subscribe_commands`].
    ///
    /// Use [`run_managed_with_state`] to receive these messages in a callback.
    pub async fn subscribe_state(&self, device_id: &str) -> Result<()> {
        let topic_full = format!("homecore/devices/{device_id}/state");
        let topic_partial = format!("homecore/devices/{device_id}/state/partial");
        self.client
            .subscribe(&topic_full, QoS::AtLeastOnce)
            .await
            .context("subscribe_state (full) failed")?;
        self.client
            .subscribe(&topic_partial, QoS::AtLeastOnce)
            .await
            .context("subscribe_state (partial) failed")?;
        {
            let mut subs = self.subscriptions.lock().unwrap();
            subs.insert(topic_full);
            subs.insert(topic_partial);
        }
        debug!(
            device_id,
            "Subscribed to external device state (full + partial)"
        );
        Ok(())
    }

    /// Remove a subscription previously added with [`subscribe_state`].
    pub async fn unsubscribe_state(&self, device_id: &str) -> Result<()> {
        let topic_full = format!("homecore/devices/{device_id}/state");
        let topic_partial = format!("homecore/devices/{device_id}/state/partial");
        let _ = self.client.unsubscribe(&topic_full).await;
        let _ = self.client.unsubscribe(&topic_partial).await;
        let mut subs = self.subscriptions.lock().unwrap();
        subs.remove(&topic_full);
        subs.remove(&topic_partial);
        debug!(device_id, "Unsubscribed from external device state");
        Ok(())
    }

    // ── Event loop ───────────────────────────────────────────────────────

    /// Drive the MQTT event loop, calling `on_command` whenever a `cmd`
    /// message arrives for any subscribed device.
    ///
    /// This method blocks until the connection is lost or an error occurs.
    pub async fn run<F>(mut self, on_command: F) -> Result<()>
    where
        F: Fn(String, Value) + Send + Sync + 'static,
    {
        self.run_inner(on_command, noop_state_cb, None).await
    }

    /// Like [`run`], but also handles management protocol commands (heartbeat
    /// responses, config read/write, log level changes).
    ///
    /// Pass the [`ManagementHandle`] returned by [`enable_management`].
    pub async fn run_managed<F>(mut self, on_command: F, mgmt: ManagementHandle) -> Result<()>
    where
        F: Fn(String, Value) + Send + Sync + 'static,
    {
        self.run_inner(on_command, noop_state_cb, Some(mgmt)).await
    }

    /// Like [`run_managed`], but additionally delivers state updates for any
    /// device subscribed to via [`subscribe_state`] into `on_state`.
    ///
    /// Use for cross-device consumer plugins (e.g. thermostat observes sensors).
    pub async fn run_managed_with_state<F, S>(
        mut self,
        on_command: F,
        on_state: S,
        mgmt: ManagementHandle,
    ) -> Result<()>
    where
        F: Fn(String, Value) + Send + Sync + 'static,
        S: Fn(String, Value) + Send + Sync + 'static,
    {
        self.run_inner(on_command, on_state, Some(mgmt)).await
    }

    async fn run_inner<F, S>(
        &mut self,
        on_command: F,
        on_state: S,
        mgmt: Option<ManagementHandle>,
    ) -> Result<()>
    where
        F: Fn(String, Value) + Send + Sync + 'static,
        S: Fn(String, Value) + Send + Sync + 'static,
    {
        let plugin_id = self.config.plugin_id.clone();
        let subs = Arc::clone(&self.subscriptions);
        info!(plugin_id = %plugin_id, "Plugin event loop starting");
        loop {
            match self.eventloop.poll().await {
                Ok(rumqttc::Event::Incoming(Packet::ConnAck(_))) => {
                    info!("Plugin connected to broker");
                    // Re-subscribe to all tracked topics on every (re)connect.
                    // With clean_session=true, subscriptions are lost on reconnect.
                    let topics: Vec<String> = subs.lock().unwrap().iter().cloned().collect();
                    for topic in &topics {
                        if let Err(e) = self
                            .client
                            .subscribe(topic.as_str(), QoS::AtLeastOnce)
                            .await
                        {
                            error!(topic, error = %e, "Failed to re-subscribe on reconnect");
                        }
                    }
                    if !topics.is_empty() {
                        info!(
                            count = topics.len(),
                            "Re-subscribed to {} topics",
                            topics.len()
                        );
                    }
                    // Republish capability manifest retained, if declared.
                    // Retained so late-joining core instances still see it.
                    if let Some(ref mgmt) = mgmt {
                        if let Some(ref caps) = mgmt.capabilities {
                            let topic =
                                format!("homecore/plugins/{}/capabilities", mgmt.plugin_id);
                            match serde_json::to_vec(caps) {
                                Ok(bytes) => {
                                    if let Err(e) = self
                                        .client
                                        .publish(&topic, QoS::AtLeastOnce, true, bytes)
                                        .await
                                    {
                                        warn!(error = %e, "Failed to publish capabilities");
                                    }
                                }
                                Err(e) => {
                                    warn!(error = %e, "Failed to serialise capabilities");
                                }
                            }
                        }
                    }
                }
                Ok(rumqttc::Event::Incoming(Packet::Publish(p))) => {
                    let parts: Vec<&str> = p.topic.split('/').collect();

                    // homecore/devices/{id}/cmd
                    if parts.len() == 4
                        && parts[0] == "homecore"
                        && parts[1] == "devices"
                        && parts[3] == "cmd"
                    {
                        let device_id = parts[2].to_string();
                        match serde_json::from_slice::<Value>(&p.payload) {
                            Ok(cmd) => on_command(device_id, cmd),
                            Err(e) => warn!(topic = %p.topic, error = %e, "Non-JSON cmd payload"),
                        }
                        continue;
                    }

                    // homecore/devices/{id}/state (full state, for subscribe_state consumers)
                    if parts.len() == 4
                        && parts[0] == "homecore"
                        && parts[1] == "devices"
                        && parts[3] == "state"
                    {
                        let device_id = parts[2].to_string();
                        match serde_json::from_slice::<Value>(&p.payload) {
                            Ok(state) => on_state(device_id, state),
                            Err(e) => warn!(topic = %p.topic, error = %e, "Non-JSON state payload"),
                        }
                        continue;
                    }

                    // homecore/devices/{id}/state/partial (merge patch — delivered
                    // to the same callback so cross-device consumers that read
                    // specific attributes see updates between full-state pushes).
                    if parts.len() == 5
                        && parts[0] == "homecore"
                        && parts[1] == "devices"
                        && parts[3] == "state"
                        && parts[4] == "partial"
                    {
                        let device_id = parts[2].to_string();
                        match serde_json::from_slice::<Value>(&p.payload) {
                            Ok(state) => on_state(device_id, state),
                            Err(e) => {
                                warn!(topic = %p.topic, error = %e, "Non-JSON state/partial payload")
                            }
                        }
                        continue;
                    }

                    // homecore/plugins/{id}/manage/cmd
                    if let Some(ref mgmt) = mgmt {
                        if parts.len() == 5
                            && parts[0] == "homecore"
                            && parts[1] == "plugins"
                            && parts[3] == "manage"
                            && parts[4] == "cmd"
                        {
                            if let Ok(cmd) = serde_json::from_slice::<Value>(&p.payload) {
                                let resp = dispatch_management_cmd(
                                    mgmt,
                                    &self.client,
                                    &cmd,
                                )
                                .await;
                                let resp_topic =
                                    format!("homecore/plugins/{}/manage/response", mgmt.plugin_id);
                                let _ = self
                                    .client
                                    .publish(
                                        &resp_topic,
                                        QoS::AtLeastOnce,
                                        false,
                                        serde_json::to_vec(&resp).unwrap_or_default(),
                                    )
                                    .await;
                            }
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    error!(error = %e, "Plugin MQTT error; retrying");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }
}

/// Top-level management-cmd dispatcher. Handles streaming actions
/// (`action` matches a registered `StreamingAction`), `cancel`, and
/// `respond` before falling through to the sync built-in handler.
async fn dispatch_management_cmd(
    mgmt: &ManagementHandle,
    client: &AsyncClient,
    cmd: &Value,
) -> Value {
    let action = cmd["action"].as_str().unwrap_or("").to_string();
    let request_id = cmd["request_id"].as_str().unwrap_or("").to_string();

    // `cancel` — flip the cancel flag on the targeted active stream.
    if action == "cancel" {
        let Some(target) = cmd["target_request_id"].as_str() else {
            return json!({
                "request_id": request_id,
                "status": "error",
                "error": "cancel requires target_request_id",
            });
        };
        let found = {
            let map = mgmt.active_streams.lock().unwrap();
            if let Some(entry) = map.get(target) {
                entry.cancel.store(true, std::sync::atomic::Ordering::SeqCst);
                true
            } else {
                false
            }
        };
        if found {
            return json!({ "request_id": request_id, "status": "ok" });
        } else {
            return json!({
                "request_id": request_id,
                "status": "error",
                "error": "no active stream for target_request_id",
            });
        }
    }

    // `respond` — deliver response payload to the targeted active stream.
    if action == "respond" {
        let Some(target) = cmd["target_request_id"].as_str() else {
            return json!({
                "request_id": request_id,
                "status": "error",
                "error": "respond requires target_request_id",
            });
        };
        let response = cmd
            .get("response")
            .cloned()
            .unwrap_or_else(|| Value::Object(Default::default()));
        let delivered = {
            let map = mgmt.active_streams.lock().unwrap();
            match map.get(target) {
                Some(entry) => entry.respond_tx.send(response).is_ok(),
                None => false,
            }
        };
        if delivered {
            return json!({ "request_id": request_id, "status": "ok" });
        } else {
            return json!({
                "request_id": request_id,
                "status": "error",
                "error": "no active awaiting_user stream for target_request_id",
            });
        }
    }

    // Streaming action match?
    if let Some(streaming_action) = mgmt.streaming_actions.get(&action) {
        return dispatch_streaming_action(mgmt, client, streaming_action, cmd).await;
    }

    // Fall through to synchronous built-ins + custom handler.
    handle_management_cmd(mgmt, cmd)
}

/// Dispatch a streaming action: register state, spawn the closure, and
/// return the sync `accepted` reply with the stream topic.
async fn dispatch_streaming_action(
    mgmt: &ManagementHandle,
    client: &AsyncClient,
    action: &StreamingAction,
    cmd: &Value,
) -> Value {
    let request_id = cmd["request_id"].as_str().unwrap_or("").to_string();
    if request_id.is_empty() {
        return json!({
            "status": "error",
            "error": "streaming action requires request_id on the command",
        });
    }

    // Extract params: everything except action/request_id/target_request_id.
    let params = {
        let mut p = cmd.clone();
        if let Some(obj) = p.as_object_mut() {
            obj.remove("action");
            obj.remove("request_id");
            obj.remove("target_request_id");
        }
        p
    };

    let stream_topic = format!(
        "homecore/plugins/{}/commands/{}/events",
        mgmt.plugin_id, request_id
    );
    let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let terminal = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (respond_tx, respond_rx) = tokio::sync::mpsc::unbounded_channel::<Value>();

    {
        let mut map = mgmt.active_streams.lock().unwrap();
        map.insert(
            request_id.clone(),
            streaming::ActiveStreamEntry {
                cancel: Arc::clone(&cancel),
                respond_tx,
            },
        );
    }

    let ctx = StreamContext::new(
        request_id.clone(),
        mgmt.plugin_id.clone(),
        action.id.clone(),
        client.clone(),
        Arc::clone(&cancel),
        Arc::clone(&terminal),
        respond_rx,
    );

    let handler = Arc::clone(&action.handler);
    let registry = Arc::clone(&mgmt.active_streams);
    let rid_clone = request_id.clone();
    let topic_clone = stream_topic.clone();
    let terminal_clone = Arc::clone(&terminal);
    let client_clone = client.clone();

    tokio::spawn(async move {
        let result = handler(ctx, params).await;
        streaming::finalize_stream(
            &client_clone,
            &topic_clone,
            &rid_clone,
            &terminal_clone,
            result,
        )
        .await;
        // Clean up registry entry after finalize — any late cancel/respond
        // after terminal becomes a no-op.
        let mut map = registry.lock().unwrap();
        map.remove(&rid_clone);
    });

    json!({
        "request_id": request_id,
        "status": "accepted",
        "stream_topic": stream_topic,
    })
}

/// Handle a management command and return a JSON response.
fn handle_management_cmd(mgmt: &ManagementHandle, cmd: &Value) -> Value {
    let action = cmd["action"].as_str().unwrap_or("");
    let request_id = cmd["request_id"].as_str().unwrap_or("").to_string();

    match action {
        "ping" => serde_json::json!({
            "request_id": request_id,
            "status": "ok",
        }),
        "get_config" => {
            if let Some(ref path) = mgmt.config_path {
                match std::fs::read_to_string(path) {
                    Ok(content) => serde_json::json!({
                        "request_id": request_id,
                        "status": "ok",
                        "data": content,
                    }),
                    Err(e) => serde_json::json!({
                        "request_id": request_id,
                        "status": "error",
                        "error": format!("failed to read config: {e}"),
                    }),
                }
            } else {
                serde_json::json!({
                    "request_id": request_id,
                    "status": "error",
                    "error": "no config path configured",
                })
            }
        }
        "set_config" => {
            if let Some(ref path) = mgmt.config_path {
                let config_str = if let Some(s) = cmd["config"].as_str() {
                    s.to_string()
                } else if let Some(obj) = cmd["config"].as_object() {
                    // JSON object → TOML
                    let toml_val: toml::Value =
                        match serde_json::from_value(Value::Object(obj.clone())) {
                            Ok(v) => v,
                            Err(e) => {
                                return serde_json::json!({
                                    "request_id": request_id,
                                    "status": "error",
                                    "error": format!("invalid config: {e}"),
                                })
                            }
                        };
                    toml::to_string_pretty(&toml_val).unwrap_or_default()
                } else {
                    return serde_json::json!({
                        "request_id": request_id,
                        "status": "error",
                        "error": "missing 'config' field",
                    });
                };
                match std::fs::write(path, &config_str) {
                    Ok(()) => serde_json::json!({
                        "request_id": request_id,
                        "status": "ok",
                    }),
                    Err(e) => serde_json::json!({
                        "request_id": request_id,
                        "status": "error",
                        "error": format!("failed to write config: {e}"),
                    }),
                }
            } else {
                serde_json::json!({
                    "request_id": request_id,
                    "status": "error",
                    "error": "no config path configured",
                })
            }
        }
        "set_log_level" => {
            let level = cmd["level"].as_str().unwrap_or("info");
            if let Some(ref handle) = mgmt.log_level_handle {
                match handle.set_level(level) {
                    Ok(()) => {
                        info!(level, "Management: log level changed dynamically");
                        serde_json::json!({
                            "request_id": request_id,
                            "status": "ok",
                        })
                    }
                    Err(e) => serde_json::json!({
                        "request_id": request_id,
                        "status": "error",
                        "error": e,
                    }),
                }
            } else {
                info!(
                    level,
                    "Management: log level change requested (no reload handle; requires restart)"
                );
                serde_json::json!({
                    "request_id": request_id,
                    "status": "ok",
                    "note": "log level change acknowledged; restart required to take effect",
                })
            }
        }
        _ => {
            if let Some(ref h) = mgmt.custom_handler {
                if let Some(mut resp) = h(cmd) {
                    resp["request_id"] = Value::String(request_id);
                    return resp;
                }
            }
            serde_json::json!({
                "request_id": request_id,
                "status": "error",
                "error": format!("unknown action: {action}"),
            })
        }
    }
}
