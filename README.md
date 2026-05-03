# hc-plugin-sdk-rs

Rust plugin SDK for HomeCore. Provides async MQTT client, device registration, state publishing, and management protocol support.

## Quick start

```toml
[dependencies]
plugin-sdk-rs = { path = "../sdks/hc-plugin-sdk-rs" }
tokio = { version = "1", features = ["full"] }
```

```rust
use plugin_sdk_rs::PluginClient;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut client = PluginClient::connect(
        "plugin.example",
        "127.0.0.1",
        1883,
        "",
    ).await?;

    let publisher = client.device_publisher();
    publisher.register_device_full("example_sensor", "Example Sensor", Some("sensor"), None, None).await?;
    publisher.publish_state("example_sensor", &serde_json::json!({"temperature": 21.5})).await?;

    client.run_command_loop(|device_id, payload| async move {
        println!("Command for {device_id}: {payload}");
        Ok(())
    }).await
}
```

## Features

- **PluginClient** — async MQTT connection with automatic reconnect and topic re-subscription
- **DevicePublisher** — cloneable handle for publishing state from spawned tasks
- **Device registration** — full schema or typed registration
- **State publishing** — full (retained) and partial (merge-patch) with optional provenance metadata
- **Management protocol** — heartbeat, remote config, dynamic log level
- **Log forwarding** — `MqttLogLayer` forwards tracing logs to core via MQTT

## Secrets in log fields

`MqttLogLayer` publishes log events to a topic anything can subscribe to,
so secret values must not leak into them. The layer redacts any field
whose name (case-insensitive) contains `password`, `secret`, `token`,
`key`, `psk`, `passcode`, `credential`, or `auth`. Redacted fields are
still emitted with the same name; only the value becomes `<redacted>`.

**Convention:** pass secrets as named tracing fields, never interpolate
them into the message string. Only field *names* are filtered — message
text is published as-is.

```rust
// Good — value is redacted automatically:
tracing::info!(api_key = %config.api_key, "Connecting to bridge");

// Bad — message is published verbatim:
tracing::info!("Connecting with key {}", config.api_key);
```
