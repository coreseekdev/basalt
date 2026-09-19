#!/usr/bin/env python3
"""协议字段类型审计（账本 66 回归门禁）。

扫描 server handler 对协议字段的 as_iXX 访问，与 protocol/definition/
官方 JSON 声明的类型逐一比对——变体窄匹配（如 I16 用 as_i32 读）会静默
返 0（㊿/66 家族：解码器按变体窄匹配，跨变体兜底 = 静默零值）。

退出码：0 = 无错配；1 = 有错配（打印清单）。
"""
import collections
import glob
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFS = os.path.join(ROOT, "protocol", "definition")
SRC = [os.path.join(ROOT, "server", "src")]

type_of = collections.defaultdict(set)


def walk(fields):
    for f in fields:
        if not isinstance(f, dict):
            continue
        t, name = f.get("type", ""), f.get("name", "")
        if name and t in ("int8", "int16", "int32", "int64"):
            type_of[name].add(t)
        if isinstance(f.get("fields"), list):
            walk(f["fields"])


for path in glob.glob(os.path.join(DEFS, "*Request.json")):
    try:
        txt = re.sub(r"//[^\n]*", "", open(path).read())
        walk(json.loads(txt).get("fields", []))
    except Exception:
        pass

norm = {"int8": "8", "int16": "16", "int32": "32", "int64": "64"}
issues = []
for directory in SRC:
    for path in glob.glob(os.path.join(directory, "*.rs")):
        src = open(path).read()
        for m in re.finditer(r'get\("(\w+)"\)[^;]{0,80}?\.as_(i8|i16|i32|i64)\(', src):
            name, ty = m.group(1), m.group(2)
            types = type_of.get(name)
            if types and ty.lstrip("i") not in {norm[t] for t in types if t in norm}:
                line = src[: m.start()].count("\n") + 1
                issues.append((path, line, name, ty, sorted(types)))

for path, line, name, ty, types in issues:
    print(f"{path}:{line}  get(\"{name}\").as_{ty}()  官方={types}")
print(f"audit: {len(issues)} mismatch(es)")
sys.exit(1 if issues else 0)
