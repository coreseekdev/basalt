//! 协议数据通用树：decode 的产物、encode 的原料。
//!
//! 设计取舍：
//! - 元数据字段用 owned 小对象（i32/String/Vec）——数量小、handler 易写；
//! - 大块数据（records/bytes）用 `bytes::Bytes` 视图——produce→存储→fetch
//!   全链路零拷贝透传（KafScale 同款：broker 不解记录体）。

use bytes::Bytes;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U16(u16),
    U32(u32),
    F64(f64),
    Str(Box<str>),
    Bytes(Bytes),
    Uuid(u128),
    Array(Vec<Value>),
    /// 结构体：字段按 (名称, 值)；tagged fields 单独存放（编码时进 tag section）。
    Struct(Struct),
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Struct {
    pub fields: Vec<(Box<str>, Value)>,
    pub tags: Vec<(i32, Value)>,
}

impl Struct {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&mut self, name: &str, v: Value) -> &mut Self {
        self.fields.push((name.into(), v));
        self
    }

    pub fn set_tag(&mut self, tag: i32, v: Value) -> &mut Self {
        self.tags.push((tag, v));
        self
    }

    pub fn get(&self, name: &str) -> Option<&Value> {
        self.fields.iter().find(|(n, _)| n.as_ref() == name).map(|(_, v)| v)
    }

    /// 取必填字段（缺失视为协议错误）。
    pub fn req(&self, name: &str) -> &Value {
        self.get(name).unwrap_or_else(|| {
            panic!("protocol: missing required field `{name}` (schema/handler mismatch)")
        })
    }

    pub fn get_struct(&self, name: &str) -> Option<&Struct> {
        match self.get(name) {
            Some(Value::Struct(s)) => Some(s),
            _ => None,
        }
    }

    pub fn get_array(&self, name: &str) -> Option<&[Value]> {
        match self.get(name) {
            Some(Value::Array(a)) => Some(a),
            _ => None,
        }
    }
}

impl Value {
    pub fn str(s: impl Into<Box<str>>) -> Value {
        Value::Str(s.into())
    }

    pub fn as_i8(&self) -> i8 {
        match *self {
            Value::I8(v) => v,
            _ => 0,
        }
    }
    pub fn as_i16(&self) -> i16 {
        match *self {
            Value::I16(v) => v,
            _ => 0,
        }
    }
    pub fn as_i32(&self) -> i32 {
        match *self {
            Value::I32(v) => v,
            _ => 0,
        }
    }
    pub fn as_i64(&self) -> i64 {
        match *self {
            Value::I64(v) => v,
            _ => 0,
        }
    }
    pub fn as_bool(&self) -> bool {
        match *self {
            Value::Bool(v) => v,
            _ => false,
        }
    }
    pub fn as_str(&self) -> &str {
        match self {
            Value::Str(s) => s,
            _ => "",
        }
    }
    pub fn as_bytes(&self) -> Option<&Bytes> {
        match self {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }
    pub fn as_uuid(&self) -> u128 {
        match *self {
            Value::Uuid(v) => v,
            _ => 0,
        }
    }
}

/// 便捷构造：Struct。
///
/// ```
/// use basalt_protocol::value::{Value, s};
/// let st = s([("Acks", Value::I16(-1))]);
/// ```
pub fn s<const N: usize>(fields: [(&str, Value); N]) -> Value {
    Value::Struct(Struct {
        fields: fields.map(|(n, v)| (Box::from(n), v)).into(),
        tags: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn struct_get_set() {
        let mut st = Struct::new();
        st.set("Acks", Value::I16(-1)).set("Timeout", Value::I32(30_000));
        assert_eq!(st.req("Acks").as_i16(), -1);
        assert_eq!(st.get("nope"), None);
    }
}
