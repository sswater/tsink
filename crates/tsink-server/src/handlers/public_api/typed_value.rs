//! Type-tagged JSON DTO for values exposed by the public rows/select endpoints.
//!
//! Wire shape (see `docs/superpowers/specs/2026-10-30-http-bytes-query-design.md` §4):
//!
//! ```json
//! {"type": "bytes",  "value": "yv4="}
//! {"type": "f64",    "value": 21.5}
//! {"type": "i64",    "value": 1}
//! {"type": "u64",    "value": 42}
//! {"type": "bool",   "value": true}
//! {"type": "string", "value": "text"}
//! ```
//!
//! `bytes` values are base64 (STANDARD alphabet, padded) strings, never JSON
//! arrays of numbers. Strings are plain UTF-8; serde_json emits non-ASCII
//! characters unescaped.

// Consumed by the rows/select handlers added in follow-up tasks; until they
// land, nothing outside this module's unit tests references these items.
#![allow(dead_code)]

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use serde::de::{self, MapAccess, Visitor};
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// Maximum encoded size of a single value (64 KiB).
pub(crate) const MAX_VALUE_BYTES: usize = 64 * 1024;
/// Maximum rows accepted by a single write request.
pub(crate) const MAX_ROWS_PER_REQUEST: usize = 10_000;
/// Maximum request body size (32 MiB).
pub(crate) const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;
/// Maximum timestamps accepted by a single point-query request.
pub(crate) const MAX_TIMESTAMPS: usize = 10_000;
/// Maximum `limit` accepted by a range query.
pub(crate) const MAX_LIMIT: usize = 100_000;

const FIELD_TYPE: &str = "type";
const FIELD_VALUE: &str = "value";

/// Type-tagged value exchanged over the public HTTP JSON API.
///
/// Native histograms have no DTO variant: [`from_tsink_value`] rejects them so
/// handlers can answer `400`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TypedValueDto {
    Bytes(Vec<u8>),
    F64(f64),
    I64(i64),
    U64(u64),
    Bool(bool),
    Str(String),
}

impl Serialize for TypedValueDto {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut object = serializer.serialize_struct("TypedValueDto", 2)?;
        match self {
            Self::Bytes(bytes) => {
                object.serialize_field(FIELD_TYPE, "bytes")?;
                object.serialize_field(FIELD_VALUE, &BASE64_STANDARD.encode(bytes))?;
            }
            Self::F64(value) => {
                object.serialize_field(FIELD_TYPE, "f64")?;
                object.serialize_field(FIELD_VALUE, value)?;
            }
            Self::I64(value) => {
                object.serialize_field(FIELD_TYPE, "i64")?;
                object.serialize_field(FIELD_VALUE, value)?;
            }
            Self::U64(value) => {
                object.serialize_field(FIELD_TYPE, "u64")?;
                object.serialize_field(FIELD_VALUE, value)?;
            }
            Self::Bool(value) => {
                object.serialize_field(FIELD_TYPE, "bool")?;
                object.serialize_field(FIELD_VALUE, value)?;
            }
            Self::Str(value) => {
                object.serialize_field(FIELD_TYPE, "string")?;
                object.serialize_field(FIELD_VALUE, value)?;
            }
        }
        object.end()
    }
}

impl<'de> Deserialize<'de> for TypedValueDto {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(TypedValueVisitor)
    }
}

struct TypedValueVisitor;

impl<'de> Visitor<'de> for TypedValueVisitor {
    type Value = TypedValueDto;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a `type`/`value` object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut type_tag: Option<String> = None;
        let mut raw_value: Option<serde_json::Value> = None;

        while let Some(field) = map.next_key::<String>()? {
            match field.as_str() {
                FIELD_TYPE => {
                    if type_tag.is_some() {
                        return Err(de::Error::duplicate_field(FIELD_TYPE));
                    }
                    type_tag = Some(map.next_value()?);
                }
                FIELD_VALUE => {
                    if raw_value.is_some() {
                        return Err(de::Error::duplicate_field(FIELD_VALUE));
                    }
                    raw_value = Some(map.next_value()?);
                }
                _ => {
                    map.next_value::<de::IgnoredAny>()?;
                }
            }
        }

        let type_tag = type_tag.ok_or_else(|| de::Error::missing_field(FIELD_TYPE))?;
        let raw_value = raw_value.ok_or_else(|| de::Error::missing_field(FIELD_VALUE))?;
        from_tagged_json(&type_tag, raw_value)
    }
}

fn from_tagged_json<E: de::Error>(
    type_tag: &str,
    raw_value: serde_json::Value,
) -> Result<TypedValueDto, E> {
    match type_tag {
        "bytes" => {
            let encoded = raw_value
                .as_str()
                .ok_or_else(|| E::custom("bytes value must be a base64 string"))?;
            let bytes = BASE64_STANDARD
                .decode(encoded)
                .map_err(|err| E::custom(format!("invalid base64 value: {err}")))?;
            Ok(TypedValueDto::Bytes(bytes))
        }
        "f64" => raw_value
            .as_f64()
            .map(TypedValueDto::F64)
            .ok_or_else(|| E::custom("f64 value must be a JSON number")),
        "i64" => raw_value
            .as_i64()
            .map(TypedValueDto::I64)
            .ok_or_else(|| E::custom("i64 value must be a JSON integer")),
        "u64" => raw_value
            .as_u64()
            .map(TypedValueDto::U64)
            .ok_or_else(|| E::custom("u64 value must be a non-negative JSON integer")),
        "bool" => raw_value
            .as_bool()
            .map(TypedValueDto::Bool)
            .ok_or_else(|| E::custom("bool value must be a JSON boolean")),
        "string" => raw_value
            .as_str()
            .map(|value| TypedValueDto::Str(value.to_string()))
            .ok_or_else(|| E::custom("string value must be a JSON string")),
        other => Err(E::custom(format!("unknown value type `{other}`"))),
    }
}

/// Converts an engine value into its HTTP DTO. Histograms are unsupported.
pub(crate) fn from_tsink_value(value: &tsink::Value) -> Result<TypedValueDto, &'static str> {
    match value {
        tsink::Value::F64(value) => Ok(TypedValueDto::F64(*value)),
        tsink::Value::I64(value) => Ok(TypedValueDto::I64(*value)),
        tsink::Value::U64(value) => Ok(TypedValueDto::U64(*value)),
        tsink::Value::Bool(value) => Ok(TypedValueDto::Bool(*value)),
        tsink::Value::Bytes(value) => Ok(TypedValueDto::Bytes(value.clone())),
        tsink::Value::String(value) => Ok(TypedValueDto::Str(value.clone())),
        tsink::Value::Histogram(_) => Err("histogram values are not supported by this endpoint"),
    }
}

/// Converts an HTTP DTO into an engine value. Always succeeds.
pub(crate) fn into_tsink_value(value: TypedValueDto) -> tsink::Value {
    match value {
        TypedValueDto::Bytes(value) => tsink::Value::Bytes(value),
        TypedValueDto::F64(value) => tsink::Value::F64(value),
        TypedValueDto::I64(value) => tsink::Value::I64(value),
        TypedValueDto::U64(value) => tsink::Value::U64(value),
        TypedValueDto::Bool(value) => tsink::Value::Bool(value),
        TypedValueDto::Str(value) => tsink::Value::String(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_value_roundtrips_all_variants() {
        let cases = [
            TypedValueDto::Bytes(vec![0xCA, 0xFE]),
            TypedValueDto::F64(21.5),
            TypedValueDto::I64(-1),
            TypedValueDto::U64(42),
            TypedValueDto::Bool(true),
            TypedValueDto::Str("text".to_string()),
        ];

        for case in cases {
            let encoded = serde_json::to_string(&case).expect("serialize should succeed");
            let decoded: TypedValueDto =
                serde_json::from_str(&encoded).expect("deserialize should succeed");
            assert_eq!(decoded, case);
        }
    }

    #[test]
    fn bytes_value_is_base64_not_number_array() {
        let encoded = serde_json::to_string(&TypedValueDto::Bytes(vec![0xCA, 0xFE]))
            .expect("serialize should succeed");
        assert_eq!(encoded, r#"{"type":"bytes","value":"yv4="}"#);
        assert!(!encoded.contains("[202"));
    }

    #[test]
    fn utf8_string_passes_through_unescaped() {
        let encoded = serde_json::to_string(&TypedValueDto::Str("中文".to_string()))
            .expect("serialize should succeed");
        assert_eq!(encoded, r#"{"type":"string","value":"中文"}"#);
        assert!(!encoded.contains("\\u"));

        let decoded: TypedValueDto =
            serde_json::from_str(&encoded).expect("deserialize should succeed");
        assert_eq!(decoded, TypedValueDto::Str("中文".to_string()));
    }

    #[test]
    fn invalid_base64_rejected() {
        let err =
            serde_json::from_str::<TypedValueDto>(r#"{"type":"bytes","value":"not_base64!!"}"#);
        assert!(err.is_err());
    }

    #[test]
    fn tsink_value_conversions_roundtrip_and_reject_histogram() {
        let cases = [
            (
                TypedValueDto::Bytes(vec![0xCA, 0xFE]),
                tsink::Value::Bytes(vec![0xCA, 0xFE]),
            ),
            (TypedValueDto::F64(21.5), tsink::Value::F64(21.5)),
            (TypedValueDto::I64(-1), tsink::Value::I64(-1)),
            (TypedValueDto::U64(42), tsink::Value::U64(42)),
            (TypedValueDto::Bool(false), tsink::Value::Bool(false)),
            (
                TypedValueDto::Str("中文".to_string()),
                tsink::Value::String("中文".to_string()),
            ),
        ];

        for (dto, value) in cases {
            assert_eq!(from_tsink_value(&value), Ok(dto.clone()));
            assert_eq!(into_tsink_value(dto), value);
        }

        let histogram = tsink::NativeHistogram {
            count: None,
            sum: 1.5,
            schema: 0,
            zero_threshold: 0.0,
            zero_count: None,
            negative_spans: Vec::new(),
            negative_deltas: Vec::new(),
            negative_counts: Vec::new(),
            positive_spans: Vec::new(),
            positive_deltas: Vec::new(),
            positive_counts: Vec::new(),
            reset_hint: tsink::HistogramResetHint::Unknown,
            custom_values: Vec::new(),
        };
        assert!(from_tsink_value(&tsink::Value::from(histogram)).is_err());
    }
}
