//! A `tracing_subscriber::Layer` that forwards log events to the HomeCore
//! broker over MQTT so they appear in the admin UI's activity stream.
//!
//! Topic: `homecore/plugins/{plugin_id}/logs`
//! Payload: JSON-serialised [`hc_types::LogLine`].
//!
//! The layer is added to the tracing subscriber at init time (before MQTT
//! connects).  Call [`MqttLogHandle::connect`] after the MQTT client is ready
//! to start forwarding.  Events before that point are silently dropped.

use hc_types::LogLine;
use rumqttc::{AsyncClient, QoS};
use std::sync::{Arc, OnceLock};
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

// ── Shared state ────────────────────────────────────────────────────────────

struct Inner {
    client: AsyncClient,
    topic: String,
    min_level: u8,
}

type SharedInner = Arc<OnceLock<Inner>>;

/// Handle used to connect the layer to MQTT after the client is ready.
/// Obtained from [`MqttLogLayer::new`].
#[derive(Clone)]
pub struct MqttLogHandle {
    inner: SharedInner,
}

impl MqttLogHandle {
    /// Activate log forwarding.  Call once after [`PluginClient::connect`].
    pub fn connect(&self, client: AsyncClient, plugin_id: &str, min_level: &str) {
        let _ = self.inner.set(Inner {
            client,
            topic: format!("homecore/plugins/{plugin_id}/logs"),
            min_level: level_to_u8(min_level),
        });
    }
}

// ── Layer ───────────────────────────────────────────────────────────────────

/// A tracing layer that publishes log lines to MQTT.
///
/// Before [`MqttLogHandle::connect`] is called, events are silently dropped.
/// Uses `try_publish` (non-blocking, QoS 0) to avoid stalling the logging
/// pipeline if the MQTT outbound queue is full.
pub struct MqttLogLayer {
    inner: SharedInner,
}

impl MqttLogLayer {
    /// Create a new layer and its associated handle.
    ///
    /// Add the layer to the tracing subscriber, then call
    /// [`MqttLogHandle::connect`] once MQTT is ready.
    pub fn new() -> (Self, MqttLogHandle) {
        let inner: SharedInner = Arc::new(OnceLock::new());
        (
            Self {
                inner: Arc::clone(&inner),
            },
            MqttLogHandle { inner },
        )
    }
}

impl<S: Subscriber> Layer<S> for MqttLogLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let state = match self.inner.get() {
            Some(s) => s,
            None => return, // not connected yet
        };

        let meta = event.metadata();

        if tracing_level_to_u8(meta.level()) < state.min_level {
            return;
        }

        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);

        let line = LogLine {
            timestamp: chrono::Utc::now(),
            level: meta.level().to_string(),
            target: meta.target().to_string(),
            message: visitor.message,
            fields: if visitor.fields.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::Value::Object(visitor.fields)
            },
        };

        if let Ok(payload) = serde_json::to_vec(&line) {
            let _ = self.client_try_publish(state, payload);
        }
    }
}

impl MqttLogLayer {
    fn client_try_publish(&self, state: &Inner, payload: Vec<u8>) {
        let _ = state.client.try_publish(
            &state.topic,
            QoS::AtMostOnce,
            false,
            payload,
        );
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn level_to_u8(level: &str) -> u8 {
    match level.to_uppercase().as_str() {
        "TRACE" => 1,
        "DEBUG" => 2,
        "INFO" => 3,
        "WARN" => 4,
        "ERROR" => 5,
        _ => 3,
    }
}

fn tracing_level_to_u8(level: &tracing::Level) -> u8 {
    match *level {
        tracing::Level::TRACE => 1,
        tracing::Level::DEBUG => 2,
        tracing::Level::INFO => 3,
        tracing::Level::WARN => 4,
        tracing::Level::ERROR => 5,
    }
}

// ── Field visitor ───────────────────────────────────────────────────────────

#[derive(Default)]
struct FieldVisitor {
    message: String,
    fields: serde_json::Map<String, serde_json::Value>,
}

impl tracing::field::Visit for FieldVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else {
            self.fields.insert(
                field.name().to_string(),
                serde_json::Value::String(value.to_string()),
            );
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let s = format!("{value:?}");
        if field.name() == "message" {
            self.message = s;
        } else {
            self.fields
                .insert(field.name().to_string(), serde_json::Value::String(s));
        }
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), serde_json::Value::Bool(value));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.fields.insert(
            field.name().to_string(),
            serde_json::Value::Number(value.into()),
        );
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.fields.insert(
            field.name().to_string(),
            serde_json::Value::Number(value.into()),
        );
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        if let Some(n) = serde_json::Number::from_f64(value) {
            self.fields
                .insert(field.name().to_string(), serde_json::Value::Number(n));
        }
    }
}
