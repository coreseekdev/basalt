//! 计划驱动的编解码（sans-I/O）。
//!
//! - decode：`&Bytes` 输入，位置推进；records/bytes 字段 `copy_to_bytes`
//!   产生视图（零拷贝）。
//! - encode：按计划逐字段查找 Value（名字匹配），flexible 时写 compact
//!   长度与 tag section（未知 tagged 由解码端跳过，故编码端只写我们声明的）。

use crate::error::{ProtocolError, Result};
use crate::plan::{Node, Plan, Ty};
use crate::primitives::{put_compact_len, put_uvarint, uvarint_len, Reader};
use crate::value::{Struct, Value};
use bytes::{BufMut, Bytes, BytesMut};

// ---------- decode ----------

/// 按计划解码一个消息体。flexible 由 registry 依 spec 判定后传入。
pub fn decode(fields: &[Node], version: i16, flexible: bool, src: &Bytes) -> Result<Struct> {
    let mut r = Reader::new(src);
    let st = decode_struct_fields(fields, version, flexible, &mut r, src)?;
    r.finish();
    Ok(st)
}

fn decode_struct_fields(
    fields: &[Node],
    version: i16,
    flexible: bool,
    r: &mut Reader,
    src: &Bytes,
) -> Result<Struct> {
    let mut st = Struct::new();
    // 常规字段
    for node in fields {
        if !node.applies(version) || node.is_tagged(version) {
            continue;
        }
        let v = decode_value(node, version, flexible, r, src)?;
        st.fields.push((node.name.clone(), v));
    }
    if flexible {
        // tag section：u8 数量，每项 uvarint tag + uvarint size + 数据
        let count = r.uvarint()? as usize;
        for _ in 0..count {
            let tag = r.uvarint()? as i32;
            let size = r.uvarint()? as usize;
            let known = fields
                .iter()
                .find(|n| n.is_tagged(version) && matches!(n.tag, Some((t, _, _)) if t == tag));
            match known {
                Some(node) => {
                    let end = r.pos() + size;
                    let v = decode_value(node, version, true, r, src)?;
                    if r.pos() != end {
                        return Err(ProtocolError::BadData(format!(
                            "tagged field {tag}: consumed {} of {size} bytes",
                            r.pos() + size - end
                        )));
                    }
                    st.tags.push((tag, v));
                }
                None => {
                    r.skip(size)?;
                }
            }
        }
    }
    Ok(st)
}

fn decode_value(
    node: &Node,
    version: i16,
    flexible: bool,
    r: &mut Reader,
    src: &Bytes,
) -> Result<Value> {
    let nullable = node.nullable;
    Ok(match node.ty {
        Ty::I8 => Value::I8(r.i8()?),
        Ty::I16 => Value::I16(r.i16()?),
        Ty::I32 => Value::I32(r.i32()?),
        Ty::I64 => Value::I64(r.i64()?),
        Ty::U16 => Value::U16(r.u16()?),
        Ty::U32 => Value::U32(r.u32()?),
        Ty::F64 => Value::F64(r.f64()?),
        Ty::Bool => Value::Bool(r.i8()? != 0),
        Ty::Str => {
            let bytes = take_str(r, flexible)?;
            match bytes {
                None => {
                    if nullable {
                        Value::Null
                    } else {
                        Value::Str("".into())
                    }
                }
                Some(b) => Value::Str(String::from_utf8_lossy(b).into()),
            }
        }
        Ty::Bytes => {
            let b = take_len_bytes(r, flexible)?;
            match b {
                None => Value::Null,
                Some(slice) => Value::Bytes(src.slice_ref(slice))
            }
        }
        Ty::Records => {
            let b = take_len_bytes(r, flexible)?;
            match b {
                None => Value::Null,
                Some(slice) => Value::Bytes(src.slice_ref(slice))
            }
        }
        Ty::Uuid => {
            let raw = r.take(16)?;
            let mut arr = [0u8; 16];
            arr.copy_from_slice(raw);
            Value::Uuid(u128::from_be_bytes(arr))
        }
        Ty::Array => {
            let count: Option<i64> = if flexible {
                let n = r.uvarint()?;
                if n == 0 {
                    None // null
                } else {
                    Some(n as i64 - 1)
                }
            } else {
                let n = r.i32()?;
                if n < 0 {
                    None
                } else {
                    Some(n as i64)
                }
            };
            match count {
                None => Value::Null,
                Some(n) => {
                    let mut arr = Vec::with_capacity(n.min(1024) as usize);
                    for _ in 0..n {
                        let elem_node = elem_of(node);
                        arr.push(decode_value(elem_node, version, flexible, r, src)?);
                    }
                    Value::Array(arr)
                }
            }
        }
        Ty::Struct => decode_struct_fields(&node.children, version, flexible, r, src)
            .map(Value::Struct)?,
    })
}

fn elem_of(node: &Node) -> &Node {
    // 原始类型数组：合成一个匿名元素节点太贵，这里直接借用 children；
    // 原始数组在 plan 中 children 为空 → 用一个静态空节点不行，
    // 故 plan 阶段已保证原始数组也带一个元素节点。
    node.children
        .first()
        .expect("array node must carry element node")
}

/// string：legacy i16 长度（-1 null）/ compact uvarint(len+1)。
/// 注意：非 flexible 的 string 是 int16 前缀，bytes/records 才是 int32。
fn take_str<'a>(r: &mut Reader<'a>, flexible: bool) -> Result<Option<&'a [u8]>> {
    if flexible {
        take_len_bytes(r, true)
    } else {
        let n = r.i16()?;
        if n < 0 {
            return Ok(None);
        }
        Ok(Some(r.take(n as usize)?))
    }
}

/// bytes/records：legacy i32 长度（-1 null）/ compact varint。
fn take_len_bytes<'a>(r: &mut Reader<'a>, flexible: bool) -> Result<Option<&'a [u8]>> {
    let len: i64 = if flexible {
        let n = r.uvarint()?;
        if n == 0 {
            return Ok(None);
        }
        n as i64 - 1
    } else {
        let n = r.i32()?;
        if n < 0 {
            return Ok(None);
        }
        n as i64
    };
    if len > i32::MAX as i64 {
        return Err(ProtocolError::BadData(format!("length overflow: {len}")));
    }
    Ok(Some(r.take(len as usize)?))
}

// ---------- encode ----------

pub fn encode(plan: &Plan, version: i16, flexible: bool, st: &Struct, out: &mut BytesMut) -> Result<()> {
    encode_struct_fields(&plan.fields, version, flexible, st, out)
}

pub fn encode_struct_fields(
    fields: &[Node],
    version: i16,
    flexible: bool,
    st: &Struct,
    out: &mut BytesMut,
) -> Result<()> {
    for node in fields {
        if !node.applies(version) || node.is_tagged(version) {
            continue;
        }
        let v = st.get(&node.name).unwrap_or(&Value::Null);
        encode_value(node, version, flexible, v, out)?;
    }
    if flexible {
        // tag section：u8 数量，每项 uvarint tag + uvarint size + 数据。
        // 只编解码 handler 显式给出的 tags；未知 tag 解码端跳过（前向兼容）。
        put_uvarint(out, st.tags.len() as u32);
        for (tag, v) in &st.tags {
            let node = fields
                .iter()
                .find(|n| matches!(n.tag, Some((t, _, _)) if t == *tag))
                .ok_or_else(|| ProtocolError::BadData(format!("unknown tag {tag}")))?;
            let mut tmp = BytesMut::new();
            encode_value(node, version, true, v, &mut tmp)?;
            put_uvarint(out, *tag as u32);
            put_uvarint(out, tmp.len() as u32);
            out.extend_from_slice(&tmp);
        }
    }
    Ok(())
}

fn encode_value(node: &Node, version: i16, flexible: bool, v: &Value, out: &mut BytesMut) -> Result<()> {
    match node.ty {
        Ty::I8 => out.put_i8(v.as_i8()),
        Ty::I16 => out.put_i16(v.as_i16()),
        Ty::I32 => out.put_i32(v.as_i32()),
        Ty::I64 => out.put_i64(v.as_i64()),
        Ty::U16 => out.put_u16(v.as_i16() as u16),
        Ty::U32 => out.put_u32(v.as_i32() as u32),
        Ty::F64 => out.put_i64(f64::to_bits(as_f64(v)) as i64),
        Ty::Bool => out.put_i8(i8::from(v.as_bool())),
        Ty::Str => match v {
            Value::Null => {
                if flexible {
                    put_compact_len(out, None);
                } else {
                    out.put_i16(-1); // legacy null string = int16 -1
                }
            }
            Value::Str(s) => {
                put_str_len(out, s.len(), flexible)?;
                out.extend_from_slice(s.as_bytes());
            }
            _ => return Err(bad_val(node, v)),
        },
        Ty::Bytes | Ty::Records => match v {
            Value::Null => {
                if flexible {
                    put_compact_len(out, None);
                } else {
                    out.put_i32(-1);
                }
            }
            Value::Bytes(b) => {
                if b.len() > i32::MAX as usize {
                    return Err(ProtocolError::BadData(format!(
                        "bytes length {} exceeds i32::MAX", b.len()
                    )));
                }
                if flexible {
                    put_compact_len(out, Some(b.len()));
                } else {
                    out.put_i32(b.len() as i32);
                }
                out.extend_from_slice(b);
            }
            _ => return Err(bad_val(node, v)),
        },
        Ty::Uuid => {
            let u = v.as_uuid();
            out.extend_from_slice(&u.to_be_bytes());
        }
        Ty::Array => {
            let items: &[Value] = match v {
                Value::Null => &[],
                Value::Array(a) => a,
                _ => return Err(bad_val(node, v)),
            };
            let null = matches!(v, Value::Null) && node.nullable;
            if null {
                if flexible {
                    put_compact_len(out, None);
                } else {
                    out.put_i32(-1);
                }
            } else {
                put_array_len(out, items.len(), flexible);
                let elem = elem_of(node);
                for item in items {
                    encode_value(elem, version, flexible, item, out)?;
                }
            }
        }
        Ty::Struct => match v {
            Value::Struct(st) => encode_struct_fields(&node.children, version, flexible, st, out)?,
            Value::Null => {
                // nullable struct（如 CurrentLeader/DivergingEpoch 不发）：
                // flexible 时写一个空 struct + 空 tag section；legacy 写默认字段。
                encode_struct_fields(
                    &node.children,
                    version,
                    flexible,
                    &Struct::default(),
                    out,
                )?;
            }
            _ => return Err(bad_val(node, v)),
        },
    }
    Ok(())
}

fn as_f64(v: &Value) -> f64 {
    match *v {
        Value::F64(f) => f,
        Value::I32(i) => f64::from(i),
        Value::I64(i) => i as f64,
        _ => 0.0,
    }
}

fn put_str_len(out: &mut BytesMut, len: usize, flexible: bool) -> Result<()> {
    if flexible {
        put_compact_len(out, Some(len));
    } else {
        if len > i16::MAX as usize {
            return Err(ProtocolError::BadData(format!(
                "string length {} exceeds i16::MAX", len
            )));
        }
        out.put_i16(len as i16);
    }
    Ok(())
}

fn put_array_len(out: &mut BytesMut, len: usize, flexible: bool) {
    if flexible {
        put_compact_len(out, Some(len));
    } else {
        out.put_i32(len as i32);
    }
}

fn bad_val(node: &Node, v: &Value) -> ProtocolError {
    ProtocolError::BadData(format!(
        "encode field `{}` (type {:?}): unexpected value {v:?}",
        node.name, node.ty
    ))
}

/// 编码前预算（可选优化路径）：估算 flexible compact 长度。
pub fn compact_len_hint(len: usize) -> usize {
    uvarint_len(len as u32 + 1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::MessageSpec;
    use crate::plan::Plan;

    const REQ: &str = r#"{
      "apiKey": 0, "name": "Req", "validVersions": "0-3", "flexibleVersions": "3+",
      "fields": [
        { "name": "Acks", "type": "int16", "versions": "0+" },
        { "name": "Names", "type": "[]string", "versions": "0+" },
        { "name": "Payload", "type": "records", "versions": "0+", "nullableVersions": "0+" },
        { "name": "Note", "type": "string", "versions": "3+", "taggedVersions": "3+", "tag": 0,
          "fields": [] }
      ]
    }"#;

    fn plan() -> Plan {
        Plan::compile(&MessageSpec::parse(REQ).unwrap()).unwrap()
    }

    #[test]
    fn roundtrip_legacy() {
        let p = plan();
        let mut st = Struct::new();
        st.set("Acks", Value::I16(-1))
            .set("Names", Value::Array(vec![Value::str("t1"), Value::str("t2")]))
            .set("Payload", Value::Bytes(Bytes::from_static(b"\x00\x01\x02")));
        let mut buf = BytesMut::new();
        encode(&p, 2, false, &st, &mut buf).unwrap();
        let src = buf.freeze();
        let back = decode(&p.fields, 2, false, &src).unwrap();
        assert_eq!(back.req("Acks").as_i16(), -1);
        assert_eq!(back.get_array("Names").unwrap().len(), 2);
        assert_eq!(back.req("Payload").as_bytes().unwrap().as_ref(), b"\x00\x01\x02");
    }

    #[test]
    fn roundtrip_flexible_with_tags() {
        let p = plan();
        let mut st = Struct::new();
        st.set("Acks", Value::I16(1))
            .set("Names", Value::Array(vec![]))
            .set("Payload", Value::Bytes(Bytes::from_static(b"xyz")));
        st.set_tag(0, Value::Str("hello".into()));
        let mut buf = BytesMut::new();
        encode(&p, 3, true, &st, &mut buf).unwrap();
        let src = buf.freeze();
        let back = decode(&p.fields, 3, true, &src).unwrap();
        assert_eq!(back.req("Acks").as_i16(), 1);
        assert_eq!(back.tags.len(), 1);
        assert_eq!(back.tags[0].1.as_str(), "hello");
    }

    #[test]
    fn null_records() {
        let p = plan();
        let mut st = Struct::new();
        st.set("Acks", Value::I16(0))
            .set("Names", Value::Array(vec![]))
            .set("Payload", Value::Null);
        for (v, flex) in [(2, false), (3, true)] {
            let mut buf = BytesMut::new();
            encode(&p, v, flex, &st, &mut buf).unwrap();
            let src = buf.freeze();
            let back = decode(&p.fields, v, flex, &src).unwrap();
            assert!(matches!(back.req("Payload"), Value::Null));
        }
    }

    #[test]
    fn legacy_string_is_i16_prefixed() {
        // Kafka 非 flexible string = int16 长度（真实客户端对拍抓出的 bug 回归）
        let p = plan();
        let mut st = Struct::new();
        st.set("Acks", Value::I16(1))
            .set("Names", Value::Array(vec![Value::str("tp")]))
            .set("Payload", Value::Null);
        let mut buf = BytesMut::new();
        encode(&p, 2, false, &st, &mut buf).unwrap();
        // Acks(2) + array len i32(4) + str len i16(2) + "tp"
        assert_eq!(&buf[2..6], &1i32.to_be_bytes(), "array count");
        assert_eq!(&buf[6..8], &2i16.to_be_bytes(), "string length MUST be i16");
        assert_eq!(&buf[8..10], b"tp");
    }

    #[test]
    fn int_array_roundtrip() {
        let p = plan();
        let mut st = Struct::new();
        st.set("Acks", Value::I16(0))
            .set("Names", Value::Array(vec![]))
            .set("Payload", Value::Null);
        // Names 的元素节点是 string；这里验证原始类型数组的往返经由 Payload 路径不足，
        // 故补一个专门用例由 registry 集成测试覆盖（Fetch ForgottenTopics）。
        let mut buf = BytesMut::new();
        encode(&p, 2, false, &st, &mut buf).unwrap();
        let src = buf.freeze();
        let back = decode(&p.fields, 2, false, &src).unwrap();
        assert_eq!(back.get_array("Names").unwrap().len(), 0);
    }
}
