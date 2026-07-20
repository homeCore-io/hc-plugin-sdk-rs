//! Typed authoring for a plugin's **config descriptor**.
//!
//! A descriptor says how a plugin's configuration should be *presented* —
//! units, conditionals, live data sources, prose — things a JSON Schema cannot
//! express. Hand-writing it as `serde_json::json!` works but fails silently:
//! a mistyped `"kind": "duraton"` or `"feild"` compiles fine and the field
//! simply never renders. These builders make the vocabulary a Rust API, so
//! typos are compile errors and the shape is correct by construction.
//!
//! Publish the result with
//! [`ManagementHandle::with_config_descriptor`](crate::ManagementHandle::with_config_descriptor):
//!
//! ```no_run
//! use plugin_sdk_rs::config_descriptor::{Cond, Descriptor, Field, Section, Source};
//!
//! let d = Descriptor::new("plugin.example")
//!     .title("Example")
//!     .section(
//!         Section::new("api", "HTTP API")
//!             .field(Field::toggle("api.enabled").label("Enable HTTP API").default(true))
//!             .field(
//!                 Field::port("api.port")
//!                     .label("Port")
//!                     .default(8080)
//!                     .visible_when(Cond::truthy("api.enabled")),
//!             ),
//!     );
//! let value = d.build();
//! ```

use serde::Serialize;
use serde_json::{json, Value};

fn is_false(b: &bool) -> bool {
    !*b
}

/// A whole descriptor: the plugin's configuration, in sections.
#[derive(Serialize, Clone, Debug)]
pub struct Descriptor {
    plugin_id: String,
    descriptor_version: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<String>,
    sections: Vec<Section>,
}

impl Descriptor {
    pub fn new(plugin_id: impl Into<String>) -> Self {
        Self {
            plugin_id: plugin_id.into(),
            descriptor_version: 1,
            title: None,
            sections: Vec::new(),
        }
    }

    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn section(mut self, section: Section) -> Self {
        self.sections.push(section);
        self
    }

    /// Serialise to the wire JSON handed to `with_config_descriptor`.
    pub fn build(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

/// A titled group of fields — one entry in the editor's section rail.
#[derive(Serialize, Clone, Debug)]
pub struct Section {
    id: String,
    title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    icon: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    help: Option<String>,
    /// Editable but kept out of the rail (bootstrap/connection plumbing).
    #[serde(skip_serializing_if = "is_false")]
    hidden: bool,
    fields: Vec<Field>,
}

impl Section {
    pub fn new(id: impl Into<String>, title: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            title: title.into(),
            icon: None,
            help: None,
            hidden: false,
            fields: Vec::new(),
        }
    }

    pub fn icon(mut self, icon: impl Into<String>) -> Self {
        self.icon = Some(icon.into());
        self
    }

    pub fn help(mut self, help: impl Into<String>) -> Self {
        self.help = Some(help.into());
        self
    }

    pub fn hidden(mut self) -> Self {
        self.hidden = true;
        self
    }

    pub fn field(mut self, field: Field) -> Self {
        self.fields.push(field);
        self
    }

    pub fn fields(mut self, fields: impl IntoIterator<Item = Field>) -> Self {
        self.fields.extend(fields);
        self
    }
}

/// `item` is polymorphic: a scalar kind for `list`, a column set for `table`.
#[derive(Serialize, Clone, Debug)]
#[serde(untagged)]
enum Item {
    Scalar(String),
    Fields(Vec<Field>),
}

/// One selectable value of an `enum` field.
#[derive(Serialize, Clone, Debug)]
pub struct Opt {
    value: Value,
    label: String,
}

impl Opt {
    pub fn new(value: impl Into<Value>, label: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            label: label.into(),
        }
    }
}

/// A live binding for a field's rows (`table`) or options (`select`).
///
/// Refs are resolved by the client. The generic ones: `devices` (the devices
/// *this plugin* owns) and `areas` (the house's rooms).
#[derive(Serialize, Clone, Debug)]
pub struct Source {
    kind: String,
    #[serde(rename = "ref")]
    reference: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    item_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    labels: Option<Value>,
}

impl Source {
    /// A core-owned resource, e.g. `devices` or `areas`.
    pub fn core_resource(reference: impl Into<String>) -> Self {
        Self {
            kind: "core_resource".into(),
            reference: reference.into(),
            item_key: None,
            labels: None,
        }
    }

    /// One of this plugin's own actions, streamed or not.
    pub fn plugin_action(reference: impl Into<String>) -> Self {
        Self {
            kind: "plugin_action".into(),
            reference: reference.into(),
            item_key: None,
            labels: None,
        }
    }

    /// Which property identifies a row.
    pub fn item_key(mut self, key: impl Into<String>) -> Self {
        self.item_key = Some(key.into());
        self
    }

    /// Which properties title/subtitle a row in the UI.
    pub fn labels(mut self, title: impl Into<String>, subtitle: impl Into<String>) -> Self {
        self.labels = Some(json!({ "title": title.into(), "subtitle": subtitle.into() }));
        self
    }
}

/// A small boolean expression over sibling field values. No code — just field
/// comparisons composed with all/any/not.
#[derive(Serialize, Clone, Debug)]
#[serde(transparent)]
pub struct Cond(Value);

impl Cond {
    /// Field is set / non-empty / true.
    pub fn truthy(field: impl Into<String>) -> Self {
        Cond(json!({ "field": field.into(), "truthy": true }))
    }
    /// Field is unset / empty / false.
    pub fn falsy(field: impl Into<String>) -> Self {
        Cond(json!({ "field": field.into(), "truthy": false }))
    }
    pub fn eq(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Cond(json!({ "field": field.into(), "eq": value.into() }))
    }
    pub fn ne(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Cond(json!({ "field": field.into(), "ne": value.into() }))
    }
    /// Field equals one of `values`.
    pub fn one_of<V: Into<Value>>(
        field: impl Into<String>,
        values: impl IntoIterator<Item = V>,
    ) -> Self {
        let vs: Vec<Value> = values.into_iter().map(Into::into).collect();
        Cond(json!({ "field": field.into(), "in": vs }))
    }
    pub fn gt(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Cond(json!({ "field": field.into(), "gt": value.into() }))
    }
    pub fn lt(field: impl Into<String>, value: impl Into<Value>) -> Self {
        Cond(json!({ "field": field.into(), "lt": value.into() }))
    }
    pub fn all(conds: impl IntoIterator<Item = Cond>) -> Self {
        Cond(json!({ "all": conds.into_iter().map(|c| c.0).collect::<Vec<_>>() }))
    }
    pub fn any(conds: impl IntoIterator<Item = Cond>) -> Self {
        Cond(json!({ "any": conds.into_iter().map(|c| c.0).collect::<Vec<_>>() }))
    }
    // Reads as the condition DSL (`Cond::not(...)`), not `std::ops::Not` — this
    // is an associated function over a Cond, not a unary operator on self.
    #[allow(clippy::should_implement_trait)]
    pub fn not(cond: Cond) -> Self {
        Cond(json!({ "not": cond.0 }))
    }
}

/// One control in a section.
///
/// Construct with the kind constructor ([`Field::toggle`], [`Field::duration`],
/// …) then refine with the builder methods. Only the attributes that apply to a
/// kind are meaningful; the rest are simply omitted from the wire JSON.
#[derive(Serialize, Clone, Debug)]
pub struct Field {
    #[serde(skip_serializing_if = "Option::is_none")]
    key: Option<String>,
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    help: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    placeholder: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    render: Option<String>,
    #[serde(rename = "default", skip_serializing_if = "Option::is_none")]
    default_value: Option<Value>,
    #[serde(skip_serializing_if = "is_false")]
    required: bool,
    #[serde(skip_serializing_if = "is_false")]
    secret: bool,
    #[serde(skip_serializing_if = "is_false")]
    read_only: bool,
    #[serde(skip_serializing_if = "is_false")]
    allow_create: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    min: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    step: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    options: Option<Vec<Opt>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    item: Option<Item>,
    #[serde(skip_serializing_if = "Option::is_none")]
    key_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<Source>,
    #[serde(skip_serializing_if = "Option::is_none")]
    href: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    visible_when: Option<Cond>,
    #[serde(skip_serializing_if = "Option::is_none")]
    required_when: Option<Cond>,
}

impl Field {
    fn of(kind: &str, key: Option<String>) -> Self {
        Self {
            key,
            kind: kind.to_string(),
            label: None,
            help: None,
            placeholder: None,
            unit: None,
            render: None,
            default_value: None,
            required: false,
            secret: false,
            read_only: false,
            allow_create: false,
            min: None,
            max: None,
            step: None,
            options: None,
            item: None,
            key_by: None,
            source: None,
            href: None,
            text: None,
            visible_when: None,
            required_when: None,
        }
    }

    fn keyed(kind: &str, key: impl Into<String>) -> Self {
        Self::of(kind, Some(key.into()))
    }

    // ── kinds ───────────────────────────────────────────────────────────────
    pub fn toggle(key: impl Into<String>) -> Self {
        Self::keyed("toggle", key)
    }
    pub fn text(key: impl Into<String>) -> Self {
        Self::keyed("text", key)
    }
    pub fn host(key: impl Into<String>) -> Self {
        Self::keyed("host", key)
    }
    pub fn port(key: impl Into<String>) -> Self {
        Self::keyed("port", key)
    }
    pub fn url(key: impl Into<String>) -> Self {
        Self::keyed("url", key)
    }
    pub fn secret(key: impl Into<String>) -> Self {
        Self::keyed("secret", key).mark_secret()
    }
    pub fn int(key: impl Into<String>) -> Self {
        Self::keyed("int", key)
    }
    pub fn number(key: impl Into<String>) -> Self {
        Self::keyed("number", key)
    }
    /// An integer duration. Pair with [`unit`](Self::unit) (`secs`, `ms`, `min`).
    pub fn duration(key: impl Into<String>) -> Self {
        Self::keyed("duration", key)
    }
    /// A fixed set of choices; add them with [`option`](Self::option).
    pub fn enumeration(key: impl Into<String>) -> Self {
        Self::keyed("enum", key)
    }
    /// A choice drawn from a live [`Source`], optionally allowing new values.
    pub fn select(key: impl Into<String>) -> Self {
        Self::keyed("select", key)
    }
    /// A list of scalars, e.g. `Field::list("sonos.manual_hosts", "host")`.
    pub fn list(key: impl Into<String>, item_kind: impl Into<String>) -> Self {
        let mut f = Self::keyed("list", key);
        f.item = Some(Item::Scalar(item_kind.into()));
        f
    }
    /// An array of objects, rendered as rows/cards. Give it columns with
    /// [`columns`](Self::columns) and, to bind live rows, a [`Source`].
    pub fn table(key: impl Into<String>) -> Self {
        Self::keyed("table", key)
    }
    /// A prose callout — no value.
    pub fn note(text: impl Into<String>) -> Self {
        let mut f = Self::of("note", None);
        f.text = Some(text.into());
        f
    }
    /// A button opening an external URL. `{client_host}` and `{some.key}` in
    /// `href` are interpolated by the client.
    pub fn link(label: impl Into<String>, href: impl Into<String>) -> Self {
        let mut f = Self::of("link", None);
        f.label = Some(label.into());
        f.href = Some(href.into());
        f
    }

    // ── refinements ─────────────────────────────────────────────────────────
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }
    pub fn help(mut self, help: impl Into<String>) -> Self {
        self.help = Some(help.into());
        self
    }
    pub fn placeholder(mut self, placeholder: impl Into<String>) -> Self {
        self.placeholder = Some(placeholder.into());
        self
    }
    pub fn unit(mut self, unit: impl Into<String>) -> Self {
        self.unit = Some(unit.into());
        self
    }
    /// Control hint within a kind: `segmented` | `dropdown` | `radio` | `pills`
    /// for enums, `table` | `cards` for tables.
    pub fn render(mut self, render: impl Into<String>) -> Self {
        self.render = Some(render.into());
        self
    }
    pub fn default(mut self, value: impl Into<Value>) -> Self {
        self.default_value = Some(value.into());
        self
    }
    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }
    pub fn mark_secret(mut self) -> Self {
        self.secret = true;
        self
    }
    pub fn read_only(mut self) -> Self {
        self.read_only = true;
        self
    }
    /// For `select`: permit a value not present in the source options.
    pub fn allow_create(mut self) -> Self {
        self.allow_create = true;
        self
    }
    /// Lower bound. Pass an integer to keep it an integer on the wire.
    pub fn min(mut self, min: impl Into<Value>) -> Self {
        self.min = Some(min.into());
        self
    }
    pub fn max(mut self, max: impl Into<Value>) -> Self {
        self.max = Some(max.into());
        self
    }
    pub fn step(mut self, step: impl Into<Value>) -> Self {
        self.step = Some(step.into());
        self
    }
    pub fn option(mut self, value: impl Into<Value>, label: impl Into<String>) -> Self {
        self.options
            .get_or_insert_with(Vec::new)
            .push(Opt::new(value, label));
        self
    }
    /// Columns of a `table`.
    pub fn columns(mut self, columns: impl IntoIterator<Item = Field>) -> Self {
        self.item = Some(Item::Fields(columns.into_iter().collect()));
        self
    }
    /// Which column identifies a `table` row (for reconciliation).
    pub fn key_by(mut self, key: impl Into<String>) -> Self {
        self.key_by = Some(key.into());
        self
    }
    pub fn source(mut self, source: Source) -> Self {
        self.source = Some(source);
        self
    }
    pub fn visible_when(mut self, cond: Cond) -> Self {
        self.visible_when = Some(cond);
        self
    }
    pub fn required_when(mut self, cond: Cond) -> Self {
        self.required_when = Some(cond);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omits_unset_attributes() {
        let f = Field::toggle("api.enabled").label("Enable").default(true);
        let v = serde_json::to_value(&f).unwrap();
        assert_eq!(v["kind"], "toggle");
        assert_eq!(v["default"], true);
        // untouched attributes must not appear on the wire
        assert!(v.get("unit").is_none());
        assert!(v.get("required").is_none());
        assert!(v.get("source").is_none());
    }

    #[test]
    fn list_and_table_items_serialise_differently() {
        let list = serde_json::to_value(Field::list("a.hosts", "host")).unwrap();
        assert_eq!(list["item"], "host");

        let table = serde_json::to_value(
            Field::table("devices")
                .key_by("device_id")
                .columns([Field::text("name").label("Name")]),
        )
        .unwrap();
        assert!(table["item"].is_array());
        assert_eq!(table["item"][0]["key"], "name");
        assert_eq!(table["key_by"], "device_id");
    }

    #[test]
    fn conditions_match_the_wire_shape() {
        let v = serde_json::to_value(Cond::one_of("api.host", ["0.0.0.0", "::"])).unwrap();
        assert_eq!(v["field"], "api.host");
        assert_eq!(v["in"][1], "::");

        let all = serde_json::to_value(Cond::all([
            Cond::truthy("api.enabled"),
            Cond::eq("mode", "advanced"),
        ]))
        .unwrap();
        assert_eq!(all["all"][0]["truthy"], true);
        assert_eq!(all["all"][1]["eq"], "advanced");
    }

    #[test]
    fn descriptor_builds_expected_envelope() {
        let d = Descriptor::new("plugin.example")
            .title("Example")
            .section(
                Section::new("api", "HTTP API")
                    .field(Field::toggle("api.enabled").default(true))
                    .field(
                        Field::port("api.port")
                            .default(8080)
                            .visible_when(Cond::truthy("api.enabled")),
                    ),
            )
            .build();

        assert_eq!(d["plugin_id"], "plugin.example");
        assert_eq!(d["descriptor_version"], 1);
        assert_eq!(d["sections"][0]["id"], "api");
        assert_eq!(d["sections"][0]["fields"][1]["visible_when"]["field"], "api.enabled");
        // `hidden: false` is a default and should not be emitted
        assert!(d["sections"][0].get("hidden").is_none());
    }
}
