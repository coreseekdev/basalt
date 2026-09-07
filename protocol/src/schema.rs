//! 上游协议 JSON 定义的解析（容忍 `//` 行注释与 `/* */` 块注释）。
//!
//! 输出 [`MessageSpec`]：一次解析，永久只读；编译为 plan 见 `plan` 模块。

use crate::error::{ProtocolError, Result};
use serde_json::Value as J;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Range {
    pub min: i16,
    pub max: i16,
}

impl Range {
    /// 解析 "0-5" / "3+" / "none" / "0"。"none" 与 "0+" 均按全范围处理时由调用方区分。
    pub fn parse(s: &str) -> Result<Range> {
        let s = s.trim();
        if let Some((a, b)) = s.split_once('-') {
            return Ok(Range { min: parse_i16(s, a)?, max: parse_i16(s, b)? });
        }
        if let Some(a) = s.strip_suffix('+') {
            return Ok(Range { min: parse_i16(s, a)?, max: i16::MAX });
        }
        if s.eq_ignore_ascii_case("none") {
            return Ok(Range { min: 0, max: i16::MAX });
        }
        let v = parse_i16(s, s)?;
        Ok(Range { min: v, max: v })
    }

    pub fn contains(&self, v: i16) -> bool {
        v >= self.min && v <= self.max
    }
}

/// 解析 "N" 形式版本号为 i16，统一错误面。
fn parse_i16(src: &str, s: &str) -> Result<i16> {
    s.parse::<i16>()
        .map_err(|e: std::num::ParseIntError| ProtocolError::Schema(format!("range `{src}`: {e}")))
}

#[derive(Debug, Clone)]
pub struct FieldSpec {
    pub name: String,
    pub ty: String, // int16 / string / records / uuid / "[]Foo" / struct 名
    pub versions: Range,
    pub nullable: Option<Range>,
    pub tag: Option<i32>,
    pub tagged_versions: Option<Range>,
    pub fields: Vec<FieldSpec>, // inline struct / array 元素 struct
}

#[derive(Debug, Clone)]
pub struct MessageSpec {
    pub name: String,
    pub api_key: Option<i16>,
    pub msg_type: String, // request | response | header
    pub valid: Range,
    pub flexible_from: Option<i16>, // flexibleVersions "3+" → Some(3)；"none" → None
    pub latest_version_unstable: bool,
    pub fields: Vec<FieldSpec>,
    pub common_structs: Vec<(String, Vec<FieldSpec>)>,
}

impl MessageSpec {
    pub fn is_flexible(&self, version: i16) -> bool {
        self.flexible_from.is_some_and(|f| version >= f)
    }

    /// 对外稳定的最大版本（latestVersionUnstable = 最高版本尚未稳定）。
    pub fn stable_max(&self) -> i16 {
        if self.latest_version_unstable {
            self.valid.max.saturating_sub(1)
        } else {
            self.valid.max
        }
    }

    /// 从（已去注释的）JSON 文本解析。
    pub fn parse(json: &str) -> Result<MessageSpec> {
        let cleaned = strip_comments(json);
        let j: J = serde_json::from_str(&cleaned)
            .map_err(|e| ProtocolError::Schema(format!("json parse: {e}")))?;
        let name = j
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ProtocolError::Schema("missing `name`".into()))?
            .to_string();
        let msg_type = j.get("type").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let api_key = j.get("apiKey").and_then(|v| v.as_i64()).map(|v| v as i16);
        let valid = Range::parse(
            j.get("validVersions").and_then(|v| v.as_str()).unwrap_or("0"),
        )?;
        let flexible_from = match j.get("flexibleVersions").and_then(|v| v.as_str()) {
            Some("none") | None => None,
            Some(s) => Some(Range::parse(s)?.min),
        };
        let latest_version_unstable =
            j.get("latestVersionUnstable").and_then(|v| v.as_bool()).unwrap_or(false);
        let fields = parse_fields(j.get("fields"))?;
        let common_structs = j
            .get("commonStructs")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|c| {
                        let n = c.get("name")?.as_str()?.to_string();
                        let f = parse_fields(c.get("fields")).ok()?;
                        Some((n, f))
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(MessageSpec {
            name,
            api_key,
            msg_type,
            valid,
            flexible_from,
            latest_version_unstable,
            fields,
            common_structs,
        })
    }
}

fn parse_fields(v: Option<&J>) -> Result<Vec<FieldSpec>> {
    let Some(arr) = v.and_then(|v| v.as_array()) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(arr.len());
    for f in arr {
        let name = f
            .get("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ProtocolError::Schema("field missing name".into()))?
            .to_string();
        let ty = f
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ProtocolError::Schema(format!("field `{name}` missing type")))?
            .to_string();
        let versions = Range::parse(f.get("versions").and_then(|v| v.as_str()).unwrap_or("0+"))?;
        let nullable = f
            .get("nullableVersions")
            .and_then(|v| v.as_str())
            .map(Range::parse)
            .transpose()?;
        let tag = f.get("tag").and_then(|v| v.as_i64()).map(|v| v as i32);
        let tagged_versions = f
            .get("taggedVersions")
            .and_then(|v| v.as_str())
            .map(Range::parse)
            .transpose()?;
        let fields = parse_fields(f.get("fields"))?;
        out.push(FieldSpec { name, ty, versions, nullable, tag, tagged_versions, fields });
    }
    Ok(out)
}

/// 字符串感知的注释剥离（协议 JSON 使用 // 与 /* */ 注释）。
fn strip_comments(src: &str) -> String {
    let b = src.as_bytes();
    let mut out = String::with_capacity(src.len());
    let mut i = 0;
    let mut in_str = false;
    while i < b.len() {
        let c = b[i];
        if in_str {
            out.push(c as char);
            if c == b'\\' && i + 1 < b.len() {
                out.push(b[i + 1] as char);
                i += 2;
                continue;
            }
            if c == b'"' {
                in_str = false;
            }
            i += 1;
            continue;
        }
        match c {
            b'"' => {
                in_str = true;
                out.push(c as char);
                i += 1;
            }
            b'/' if i + 1 < b.len() && b[i + 1] == b'/' => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'/' if i + 1 < b.len() && b[i + 1] == b'*' => {
                i += 2;
                while i + 1 < b.len() && !(b[i] == b'*' && b[i + 1] == b'/') {
                    i += 1;
                }
                i += 2;
            }
            _ => {
                // 多字节 UTF-8 直接按字节透传（注释判定只涉及 ASCII）
                let ch_len = utf8_len(c);
                out.push_str(std::str::from_utf8(&b[i..i + ch_len]).unwrap_or(""));
                i += ch_len;
            }
        }
    }
    out
}

fn utf8_len(b: u8) -> usize {
    match b {
        0x00..=0x7f => 1,
        0xc0..=0xdf => 2,
        0xe0..=0xef => 3,
        _ => 4,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"{
      // line comment
      "apiKey": 0,
      "name": "ProduceRequest",
      "validVersions": "3-13",
      "flexibleVersions": "9+", /* block
         comment */
      "fields": [
        { "name": "Acks", "type": "int16", "versions": "0+" }
      ]
    }"#;

    #[test]
    fn parse_spec() {
        let m = MessageSpec::parse(SAMPLE).unwrap();
        assert_eq!(m.name, "ProduceRequest");
        assert_eq!(m.api_key, Some(0));
        assert_eq!(m.valid, Range { min: 3, max: 13 });
        assert_eq!(m.flexible_from, Some(9));
        assert!(m.is_flexible(9) && !m.is_flexible(8));
    }

    #[test]
    fn strip_keeps_strings() {
        let out = strip_comments(r#""a//b" // trailing"#);
        assert_eq!(out.trim(), "\"a//b\"");
    }

    #[test]
    fn ranges() {
        assert!(Range::parse("8-10").unwrap().contains(9));
        assert!(!Range::parse("8-10").unwrap().contains(11));
        assert!(Range::parse("0").unwrap().contains(0));
    }
}
