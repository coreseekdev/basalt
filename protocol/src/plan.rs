//! 编解码计划：把 [`MessageSpec`] 编译成版本门控的字段树。
//!
//! 一棵树服务全部版本——运行期用 `node.versions.contains(v)` 判定字段
//! 是否参与编解码；flexible 版本另行走 compact + tagged section 分支。

use crate::error::{ProtocolError, Result};
use crate::schema::{FieldSpec, MessageSpec};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ty {
    I8,
    I16,
    I32,
    I64,
    U16,
    U32,
    F64,
    Bool,
    Str,
    Bytes,
    Records,
    Uuid,
    Array,
    Struct,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub name: Box<str>,
    pub ty: Ty,
    pub versions: (i16, i16),
    pub nullable: bool,
    /// Some((tag, tagged_from, tagged_to))：该字段在 tag section 中编解码。
    pub tag: Option<(i32, i16, i16)>,
    /// Array 元素 / Struct 子字段。
    pub children: Vec<Node>,
}

impl Node {
    pub fn applies(&self, v: i16) -> bool {
        v >= self.versions.0 && v <= self.versions.1
    }

    pub fn is_tagged(&self, v: i16) -> bool {
        matches!(self.tag, Some((_, lo, hi)) if v >= lo && v <= hi)
    }
}

#[derive(Debug)]
pub struct Plan {
    pub name: String,
    pub fields: Vec<Node>,
}

const MAX_DEPTH: u32 = 32;

impl Plan {
    pub fn compile(spec: &MessageSpec) -> Result<Plan> {
        let fields = compile_fields(&spec.fields, &spec.common_structs, 0)?;
        Ok(Plan { name: spec.name.clone(), fields })
    }
}

fn compile_fields(
    specs: &[FieldSpec],
    commons: &[(String, Vec<FieldSpec>)],
    depth: u32,
) -> Result<Vec<Node>> {
    if depth > MAX_DEPTH {
        return Err(ProtocolError::Schema("struct recursion too deep".into()));
    }
    specs.iter().map(|f| compile_node(f, commons, depth)).collect()
}

fn compile_node(f: &FieldSpec, commons: &[(String, Vec<FieldSpec>)], depth: u32) -> Result<Node> {
    let is_array = f.ty.starts_with("[]");
    let base = if is_array { &f.ty[2..] } else { f.ty.as_str() };

    let prim = |t: &str| -> Option<Ty> {
        Some(match t {
            "int8" => Ty::I8,
            "int16" => Ty::I16,
            "int32" => Ty::I32,
            "int64" => Ty::I64,
            "uint16" => Ty::U16,
            "uint32" => Ty::U32,
            "float64" => Ty::F64,
            "bool" => Ty::Bool,
            "string" => Ty::Str,
            "bytes" => Ty::Bytes,
            "records" => Ty::Records,
            "uuid" => Ty::Uuid,
            _ => return None,
        })
    };

    // 元素/自身结构字段：inline fields 优先，其次 commonStructs 按需解析（带深度防护）
    let struct_children = |name: &str| -> Result<Vec<Node>> {
        if !f.fields.is_empty() {
            compile_fields(&f.fields, commons, depth + 1)
        } else if let Some((_, cf)) = commons.iter().find(|(n, _)| n == name) {
            compile_fields(cf, commons, depth + 1)
        } else {
            Err(ProtocolError::Schema(format!(
                "field `{}`: unknown struct `{name}`",
                f.name
            )))
        }
    };

    let (ty, children): (Ty, Vec<Node>) = if is_array {
        let elem = if let Some(pt) = prim(base) {
            Node {
                name: "".into(),
                ty: pt,
                versions: (f.versions.min, f.versions.max),
                nullable: false,
                tag: None,
                children: Vec::new(),
            }
        } else {
            Node {
                name: base.into(),
                ty: Ty::Struct,
                versions: (f.versions.min, f.versions.max),
                nullable: false,
                tag: None,
                children: struct_children(base)?,
            }
        };
        (Ty::Array, vec![elem])
    } else if let Some(pt) = prim(base) {
        (pt, Vec::new())
    } else {
        (Ty::Struct, struct_children(base)?)
    };

    Ok(Node {
        name: f.name.as_str().into(),
        ty,
        versions: (f.versions.min, f.versions.max),
        nullable: f.nullable.is_some(),
        tag: match (f.tag, &f.tagged_versions) {
            (Some(t), Some(tv)) => Some((t, tv.min, tv.max)),
            _ => None,
        },
        children,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::MessageSpec;

    const SAMPLE: &str = r#"{
      "apiKey": 0, "name": "T", "validVersions": "0-5", "flexibleVersions": "3+",
      "commonStructs": [
        { "name": "Common", "versions": "0+", "fields": [ { "name": "X", "type": "int32", "versions": "0+" } ] }
      ],
      "fields": [
        { "name": "A", "type": "int16", "versions": "0+" },
        { "name": "B", "type": "string", "versions": "1+", "nullableVersions": "1+" },
        { "name": "C", "type": "Common", "versions": "0+" },
        { "name": "D", "type": "[]int32", "versions": "0+" },
        { "name": "E", "type": "tagged", "tag": 1, "taggedVersions": "3+", "versions": "3+", "fields": [
            { "name": "E1", "type": "int32", "versions": "3+" } ] }
      ]
    }"#;

    #[test]
    fn compile_and_gate() {
        let spec = MessageSpec::parse(SAMPLE).unwrap();
        let plan = Plan::compile(&spec).unwrap();
        assert_eq!(plan.fields.len(), 5);
        let b = &plan.fields[1];
        assert!(!b.applies(0) && b.applies(1));
        assert!(b.nullable);
        let c = &plan.fields[2];
        assert_eq!(c.children.len(), 1);
        let d = &plan.fields[3];
        assert_eq!(d.ty, Ty::Array);
        let e = &plan.fields[4];
        assert!(e.is_tagged(3) && !e.is_tagged(2));
    }
}
