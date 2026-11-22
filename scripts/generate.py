#!/usr/bin/env python3
"""Writes src/gen.rs from the spec repo's openrpc.json: the result and config types, the method table, and one typed
async call per served JSON-RPC method. Usage: python3 scripts/generate.py <openrpc.json>, then cargo fmt.

Numbers: every input number is a non-negative integer (slots, limits, offsets), so it is a u64. A result number is a
serde_json::Number, which keeps an integer exact and a fraction as a fraction: the spec does not say which is which."""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
doc = json.loads(Path(sys.argv[1]).read_text())
schemas: dict[str, dict] = doc["components"]["schemas"]
rpc = [m for m in doc["methods"] if "servers" not in m]
subscriptions = [m["name"] for m in doc["methods"] if "servers" in m and m["name"].endswith("Subscribe")]

KEYWORDS = {"as", "async", "await", "box", "break", "const", "continue", "crate", "do", "dyn", "else", "enum", "extern",
            "false", "final", "fn", "for", "gen", "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut",
            "override", "priv", "pub", "ref", "return", "static", "struct", "super", "trait", "true", "try", "type",
            "typeof", "unsafe", "use", "virtual", "where", "while", "yield"}
# Value lists that read better under one name wherever they appear.
PREFERRED = {json.dumps(["legacy", 0, 1]): "TransactionVersion", json.dumps(["asc", "desc"]): "SortDirection"}
NOTES = {
    "sendTransaction": "Relays a transaction the caller's own wallet already signed, as base64 or base58 text. Sent once, never retried: check the signature before sending again.",
    "swap": "Builds one unsigned transaction for the user's own wallet to sign; nothing is signed or sent here.",
    "simulateTransaction": "Simulates a transaction; nothing is signed or sent.",
}


def pascal(name: str) -> str:
    return "".join(p[:1].upper() + p[1:] for p in re.split(r"[^A-Za-z0-9]+", name) if p)


def snake(name: str) -> str:
    s = re.sub(r"[^A-Za-z0-9]+", "_", name)
    s = re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", s)
    s = re.sub(r"(?<=[A-Z])([A-Z][a-z])", r"_\1", s)
    return s.lower().strip("_")


def field_ident(key: str) -> str:
    ident = snake(key)
    return f"r#{ident}" if ident in KEYWORDS else ident


items: dict[str, str | None] = {}  # type name -> Rust source, in the order first needed
kinds: dict[str, str] = {}  # type name -> struct, enum or alias
fields_of: dict[str, list[str]] = {}  # struct name -> its field types, to decide Default
alias_of: dict[str, str] = {}  # alias name -> aliased type
enum_by_values: dict[str, str] = {}
schema_of: dict[str, str] = {}  # type name -> the schema it was made from, so one name never holds two shapes


def claim(name: str, kind: str, s: object = None) -> bool:
    """Reserves a type name; False when it is taken already by the same shape (the caller reuses it)."""
    shape = json.dumps(s, sort_keys=True)
    if name in items:
        if s is not None and schema_of.get(name) != shape:
            sys.exit(f"{name} would name two different shapes")
        return False
    items[name] = None
    kinds[name] = kind
    schema_of[name] = shape
    return True


def number(inp: bool) -> str:
    return "u64" if inp else "Number"


def ref_name(s: dict) -> str:
    return pascal(s["$ref"].rsplit("/", 1)[1])


def rs(s: dict | None, hint: str, inp: bool) -> str:
    """The Rust type for a JSON Schema. `hint` names a type if an inline object or union needs one; `inp` is true
    inside params, where numbers are u64."""
    if not s:
        return "Value"
    if "$ref" in s:
        return ref_name(s)
    if "enum" in s:
        return enum_type(s["enum"], hint)
    if "const" in s:
        c = s["const"]
        return "bool" if isinstance(c, bool) else "String" if isinstance(c, str) else number(inp)
    if "anyOf" in s:
        return union(s["anyOf"], hint, inp)
    kind = s.get("type")
    if isinstance(kind, list):
        return union([{**s, "type": one} for one in kind], hint, inp)
    if kind == "string":
        return "String"
    if kind == "number":
        return number(inp)
    if kind == "integer":
        return "i64"
    if kind == "boolean":
        return "bool"
    if kind == "null":
        return "()"
    if kind == "array":
        it = s.get("items")
        if isinstance(it, list):  # a fixed-length array is a tuple
            return "(" + ", ".join(rs(sub, f"{hint}{pascal(sub.get('title', str(i)))}", inp) for i, sub in enumerate(it)) + ")"
        return f"Vec<{rs(it, hint + 'Item', inp)}>"
    if kind == "object" or "properties" in s:
        if s.get("properties"):
            return struct(hint, s, inp)
        extra = s.get("additionalProperties")
        return f"BTreeMap<String, {rs(extra, hint + 'Value', inp)}>" if isinstance(extra, dict) and extra else "Map<String, Value>"
    return "Value"


def enum_type(values: list, hint: str, named: bool = False) -> str:
    key = json.dumps(values)
    if key in enum_by_values:
        return enum_by_values[key]
    strings = [v for v in values if isinstance(v, str)]
    if not strings:
        return "u8"  # 0 or 1: txVersion, swapTag
    name = hint if named else PREFERRED.get(key, hint)  # a name the spec declares wins
    enum_by_values[key] = name
    text = name if len(strings) == len(values) else name + "Text"
    if claim(text, "enum"):
        variants = "\n".join(f'    #[serde(rename = "{v}")]\n    {pascal(v)},' for v in strings)
        other = "    /// A value this version does not know yet, kept as sent.\n    #[serde(untagged)]\n    Other(String),"
        items[text] = f"{cfg()}#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]\n#[non_exhaustive]\npub enum {text} {{\n{variants}\n{other}\n}}\n"
    if text != name and claim(name, "enum"):
        items[name] = (f"{cfg()}#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]\n#[serde(untagged)]\n#[non_exhaustive]\n"
                       f"pub enum {name} {{\n    Text({text}),\n    Number(u8),\n}}\n")
    return name


def union(parts: list[dict], hint: str, inp: bool) -> str:
    nullable = any(p.get("type") == "null" for p in parts)
    rest = [p for p in parts if p.get("type") != "null"]
    inner = "()" if not rest else rs(rest[0], hint, inp) if len(rest) == 1 else untagged(hint, rest, inp)
    return f"Option<{inner}>" if nullable and not inner.startswith("Option<") else inner


def variant_name(s: dict, rust: str, siblings: list[dict] = ()) -> str:
    if "$ref" in s:
        return rust
    if s.get("type") == "object" and s.get("properties"):
        keys = s.get("required") or list(s["properties"])
        shared = {k for o in siblings if o is not s and o.get("type") == "object" for k in (o.get("required") or o.get("properties", {}))}
        return pascal(next((k for k in keys if k not in shared), keys[0]))
    return {"string": "Text", "number": "Number", "boolean": "Bool", "array": "Tuple" if isinstance(s.get("items"), list) else "List"}.get(s.get("type"), "Other")


def untagged(name: str, parts: list[dict], inp: bool) -> str:
    """A union: serde tries each variant in order and keeps the first that reads."""
    if not claim(name, "enum", parts):
        return name
    lines, seen = [], set()
    for i, p in enumerate(parts):
        guess = variant_name(p, "", parts)
        rust = rs(p, name + (guess if guess not in ("Text", "Number", "Bool", "Tuple", "List", "Other") else str(i)), inp)
        v = variant_name(p, rust, parts)
        if v in seen:
            v += str(i)
        seen.add(v)
        lines.append(f"    {v}{rust if rust.startswith('(') else f'({rust})'},")
    items[name] = cfg() + "#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]\n#[serde(untagged)]\n#[non_exhaustive]\npub enum " + name + " {\n" + "\n".join(lines) + "\n}\n"
    return name


required_of: dict[str, list[tuple[str, str]]] = {}  # input struct -> its required fields, for new()
optional_of: dict[str, list[str]] = {}
cfg_of: dict[str, bool] = {}
ws_only_now = False


def cfg() -> str:
    return '#[cfg(feature = "ws")]\n' if ws_only_now else ""


def field_lines(owner: str, props: dict, required: set, inp: bool) -> tuple[list[str], list[str]]:
    lines, types, idents = [], [], set()
    required_of[owner], optional_of[owner] = [], []
    for key, sub in props.items():
        t = rs(sub, owner + pascal(key), inp)
        if key not in required and not t.startswith("Option<"):
            t = f"Option<{t}>"
        ident = field_ident(key)
        if ident in idents:
            sys.exit(f"{owner}: two fields map to {ident}")
        idents.add(ident)
        attrs = []
        if ident.removeprefix("r#") != key:
            attrs.append(f'rename = "{key}"')
        if t.startswith("Option<"):
            attrs.append("default")
            if key not in required:
                attrs.append('skip_serializing_if = "Option::is_none"')
        if attrs:
            lines.append(f"    #[serde({', '.join(attrs)})]")
        lines.append(f"    pub {ident}: {t},")
        types.append(t)
        if key in required and not t.startswith("Option<"):
            required_of[owner].append((ident, t))
        else:
            optional_of[owner].append(ident)
    return lines, types


def struct(name: str, s: dict, inp: bool) -> str:
    if not claim(name, "struct", s):
        return name
    lines, types = field_lines(name, s["properties"], set(s.get("required", [])), inp)
    extra = s.get("additionalProperties")
    if isinstance(extra, dict):
        lines += ["    /// Fields the service sends beyond the ones typed here.", "    #[serde(flatten)]", "    pub extra: Map<String, Value>,"]
        types.append("Map<String, Value>")
    fields_of[name] = types
    cfg_of[name] = ws_only_now
    if inp:
        args = ", ".join(f"{i}: {'impl Into<String>' if resolved(t) == 'String' else t}" for i, t in required_of[name])
        sets = [f"{i}: {i}.into()" if resolved(t) == "String" else i for i, t in required_of[name]] + [f"{i}: None" for i in optional_of[name]]
        if isinstance(extra, dict):
            sets.append("extra: Map::new()")
        new_fns.append(f"{cfg()}impl {name} {{\n    /// The required fields; set the optional ones on the result.\n    pub fn new({args}) -> Self {{\n        Self {{ {', '.join(sets)} }}\n    }}\n}}\n")
    items[name] = "#[non_exhaustive]\npub struct " + name + " {\n" + "\n".join(lines) + "\n}\n"
    return name


new_fns: list[str] = []


def defaultable(t: str, seen: frozenset = frozenset()) -> bool:
    if t.startswith(("Option<", "Vec<", "BTreeMap<", "Map<")) or t in ("String", "bool", "u64", "u8", "i64", "Value", "()"):
        return True
    if t in alias_of:
        return defaultable(alias_of[t], seen)
    if kinds.get(t) == "struct" and t not in seen:
        return all(defaultable(f, seen | {t}) for f in fields_of[t])
    return False


# Inputs: every schema a param reaches, so its numbers are u64.
def resolved(t: str) -> str:
    while t in alias_of:
        t = alias_of[t]
    return t


def reach(s, out: set) -> None:
    if isinstance(s, dict):
        if "$ref" in s and s["$ref"].rsplit("/", 1)[1] not in out:
            out.add(s["$ref"].rsplit("/", 1)[1])
            reach(schemas[s["$ref"].rsplit("/", 1)[1]], out)
        for v in s.values():
            reach(v, out)
    elif isinstance(s, list):
        for v in s:
            reach(v, out)


def params_object(m: dict) -> dict:
    return {"properties": {p["name"]: p["schema"] for p in m["params"]}, "required": sorted(p["name"] for p in m["params"] if p["required"])}


# A by-name method whose params equal a component object takes that component as its params struct.
params_type: dict[str, str] = {}
for m in rpc:
    if m["paramStructure"] == "by-name" and m["params"]:
        want = params_object(m)
        same = [n for n, s in schemas.items() if s.get("properties") == want["properties"] and sorted(s.get("required", [])) == want["required"]]
        params_type[m["name"]] = same[0] if same else ""

inputs: set[str] = {n for n in params_type.values() if n}
for m in doc["methods"]:
    reach([p["schema"] for p in m["params"]], inputs)
reach([schemas[n] for n in params_type.values() if n], inputs)
outputs: set[str] = set()
for m in doc["methods"]:
    reach(m["result"]["schema"], outputs)
    reach(m.get("x-notification", {}).get("result"), outputs)
for name in inputs & outputs:
    if json.dumps(schemas[name]).count('"number"'):
        sys.exit(f"{name} is both a param and a result and holds numbers")

on_rpc: set[str] = set()
for m in rpc:
    reach([p["schema"] for p in m["params"]] + [m["result"]["schema"]], on_rpc)
reach([schemas[n] for n in params_type.values() if n], on_rpc)
on_rpc |= {n for n in params_type.values() if n}  # a by-name method's params struct is the component itself
ws_only = {n for n in schemas if n not in on_rpc}

for name, s in schemas.items():  # named value lists first, so an inline copy reuses the name
    if "enum" in s and all(isinstance(v, str) for v in s["enum"]) and name not in ws_only:
        enum_type(s["enum"], pascal(name), named=True)
for name, s in sorted(schemas.items(), key=lambda kv: kv[0] in ws_only):
    ws_only_now = name in ws_only
    rust_name, inp = pascal(name), name in inputs
    if s.get("type") == "object" and s.get("properties"):
        struct(rust_name, s, inp)
    elif "enum" in s or "anyOf" in s and len([p for p in s["anyOf"] if p.get("type") != "null"]) > 1:
        rs(s, rust_name, inp)
    elif claim(rust_name, "alias"):
        alias_of[rust_name] = rs(s, rust_name + "Value", inp)
        items[rust_name] = f"{cfg()}pub type {rust_name} = {alias_of[rust_name]};\n"
ws_only_now = False


def positional_arg(t: str, required: bool) -> tuple[str, str, str]:
    """The argument type of a positional param, the owned type a test example reads into, and how it is passed."""
    r = resolved(t)
    if r == "String":
        return ("&str", "String", "&{}") if required else ("Option<&str>", "Option<String>", "{}.as_deref()")
    if r in ("u64", "Option<u64>", "bool"):
        return (r, r, "{}") if required or r.startswith("Option<") else (f"Option<{r}>", f"Option<{r}>", "{}")
    if r.startswith("Vec<") and resolved(r[4:-1]) == "String":
        return ("&[String]", "Vec<String>", "&{}") if required else ("Option<&[String]>", "Option<Vec<String>>", "{}.as_deref()")
    return (f"&{t}", t, "&{}") if required else (f"Option<&{t}>", f"Option<{t}>", "{}.as_ref()")


methods_src, table, calls = [], [], []
for m in rpc:
    name, fn = m["name"], snake(m["name"])
    if not re.fullmatch(r"[a-z][a-z0-9_]*", fn):
        sys.exit(f"{name}: no Rust name")
    result = rs(m["result"]["schema"], pascal(name) + "Result", False)
    plane, batch, permit = m["tags"][0]["name"], m["x-batch"], m["x-permit"]
    doc_lines = [f"/// `{name}`: the {plane} plane; " + {"any": "batches with any method", "das": "batches only with other DAS methods", "never": "never batched"}[batch]
                 + ("; holds a node permit (at most two per batch)." if permit else ".")]
    if name in NOTES:
        doc_lines.append(f"/// {NOTES[name]}")
    if m["paramStructure"] == "by-name":
        if m["params"]:
            pname = pascal(params_type[name]) if params_type[name] else struct(pascal(name) + "Params", {"type": "object", **params_object(m)}, True)
            sig, params = f"params: &{pname}", "Some(arg(params)?)"
            calls.append((name, f"        let p: {pname} = example_named(ex)?;\n        c.{fn}(&p).await.map(drop)"))
        else:
            sig, params = "", "None"
            calls.append((name, f"        let _ = ex;\n        c.{fn}().await.map(drop)"))
    else:
        args, sent, reads, passes = [], [], [], []
        for i, p in enumerate(m["params"]):
            t = rs(p["schema"], pascal(name) + pascal(p["name"]), True)
            arg_t, owned, how = positional_arg(t, p["required"])
            ident = field_ident(p["name"])
            args.append(f"{ident}: {arg_t}")
            sent.append(f"arg(&{ident})?")
            reads.append(f"        let a{i}: {owned} = example(&ex, {i})?;")
            passes.append(how.format(f"a{i}"))
        sig = ", ".join(args)
        params = f"positional(vec![{', '.join(sent)}])" if sent else "None"
        reads = reads or ["        let _ = ex;"]
        calls.append((name, "\n".join(reads + [f"        c.{fn}({', '.join(passes)}).await.map(drop)"])))
    methods_src.append("\n".join(f"    {d}" for d in doc_lines) + f"\n    pub async fn {fn}(&self{', ' if sig else ''}{sig}) -> Result<{result}> {{\n"
                       f"        self.request(\"{name}\", {params}).await\n    }}\n")
    table.append(f"    MethodSpec {{ name: \"{name}\", plane: Plane::{pascal(plane)}, params: ParamStructure::{'ByName' if m['paramStructure'] == 'by-name' else 'ByPosition'}, "
                 f"batch: Batch::{pascal(batch)}, permit: {'true' if permit else 'false'} }},")

for name, kind in kinds.items():
    if kind == "struct":
        derive = "Debug, Clone, PartialEq, Serialize, Deserialize" + (", Default" if defaultable(name) else "")
        items[name] = ('#[cfg(feature = "ws")]\n' if cfg_of.get(name) else "") + f"#[derive({derive})]\n" + items[name]

types_src = "\n".join(src for src in items.values() if src) + "\n" + "\n".join(new_fns)
call_fns = "\n".join(f"    pub fn {snake(n)}<'a>(c: &'a Carbium, ex: Value) -> TypedCall<'a> {{\n        Box::pin(async move {{\n{body}\n        }})\n    }}\n" for n, body in calls)
call_table = "\n".join(f'        ("{n}", {snake(n)}),' for n, _ in calls)

out = f"""// Generated by scripts/generate.py from the spec repo's openrpc.json. Do not edit: a method the services do not
// serve is not offered. Results are the JSON the service sends, typed; a result number is a serde_json::Number, which
// keeps an integer exact (read it with as_u64) and a fraction as a fraction.
#![allow(clippy::large_enum_variant, clippy::too_many_arguments)]

use std::collections::BTreeMap;

use serde::{{Deserialize, Serialize}};
use serde_json::{{Map, Number, Value}};

use crate::transport::{{arg, positional}};
use crate::{{Carbium, Result}};

{types_src}
/// Which part of services.carbium.sh answers a method.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Plane {{
    /// The standard Solana methods and the fee extras.
    Rpc,
    /// The DAS methods, each taking one object in Helius field names.
    Das,
    /// quote, swap, prices and health.
    Quote,
}}

/// How a method takes its params.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ParamStructure {{
    /// A JSON array, trailing optional params left out.
    ByPosition,
    /// One JSON object.
    ByName,
}}

/// Whether a method may share a JSON-RPC batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Batch {{
    /// With any other batchable method.
    Any,
    /// Only with other DAS methods.
    Das,
    /// Never: send it alone.
    Never,
}}

/// One served JSON-RPC method and the door rules that apply to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MethodSpec {{
    pub name: &'static str,
    pub plane: Plane,
    pub params: ParamStructure,
    pub batch: Batch,
    /// Holds a node permit: a batch holds at most two permit methods.
    pub permit: bool,
}}

/// Every JSON-RPC method services.carbium.sh serves, by name.
pub const METHODS: &[MethodSpec] = &[
{chr(10).join(table)}
];

/// The WebSocket subscriptions services.carbium.sh serves.
#[cfg(feature = "ws")]
pub const SUBSCRIPTIONS: &[&str] = &[{", ".join(f'"{n}"' for n in subscriptions)}];

/// The spec of a served method, or None when the services do not serve it.
pub fn method_spec(name: &str) -> Option<&'static MethodSpec> {{
    METHODS.iter().find(|m| m.name == name)
}}

/// One typed call per served method.
impl Carbium {{
{chr(10).join(methods_src)}}}

/// Each typed call, by the wire name it must send, run from a params example: the served test reads the method name
/// each call puts on the wire. Compiled for the crate's own tests only.
#[cfg(test)]
pub(crate) mod typed_calls {{
    use super::*;
    use crate::transport::{{example, example_named}};
    use std::future::Future;
    use std::pin::Pin;

    pub type TypedCall<'a> = Pin<Box<dyn Future<Output = Result<()>> + 'a>>;
    pub type CallFn = for<'a> fn(&'a Carbium, Value) -> TypedCall<'a>;

    pub(crate) const ALL: &[(&str, CallFn)] = &[
{call_table}
    ];

{call_fns}}}
"""
target = ROOT / "src/gen.rs"
target.write_text(out)
print(f"{target.relative_to(ROOT)}: {len(rpc)} methods, {sum(1 for k in kinds.values() if k == 'struct')} structs, "
      f"{sum(1 for k in kinds.values() if k == 'enum')} enums, {len(alias_of)} aliases")
