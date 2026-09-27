"""Independent merkle_v2 value encoder, written from docs/verifiability-hash-strategy.md.

It computes the Struct and List<Struct> golden vectors pinned in
firehose-parquet/src/verify/row_encoding.rs without Arrow or fireparq code.
Run: python3 docs/audit/validation-verify-struct-vectors.py

Values are modelled as tagged Python tuples so the reference never touches Arrow:
  None                      null (any type)
  ("utf8", str)             Utf8 / LargeUtf8 / Utf8View
  ("bin", bytes)            Binary family
  ("uint", int)             any integer type
  ("list", [values])        List / LargeList / FixedSizeList
  ("struct", [values])      Struct, children in declared field order
"""

import struct
import hashlib


def u32le(n):
    return struct.pack("<I", n)


def canonical(value):
    kind, payload = value
    if kind == "utf8":
        return payload.encode("utf-8")
    if kind == "bin":
        return payload.hex().encode("ascii")
    if kind == "uint":
        return str(payload).encode("ascii")
    if kind in ("list", "struct"):
        out = u32le(len(payload))
        for child in payload:
            out += encode_value(child)
        return out
    raise ValueError(kind)


def encode_value(value):
    if value is None:
        return b"\x00"
    body = canonical(value)
    return b"\x01" + u32le(len(body)) + body


def encode_row(columns):
    out = b""
    for name, value in columns:
        out += u32le(len(name)) + name.encode("utf-8") + encode_value(value)
    return out


def show(label, value):
    print(f"{label}: {encode_value(value).hex()}")


fee = ("struct", [("utf8", "uatom"), ("utf8", "5000")])
show("struct fee {uatom, 5000}", fee)

signer = ("struct", [None, ("bin", b"\xab"), None, ("uint", 7)])
show("struct signer {null, 0xab, null, 7}", signer)

show("struct null", None)
show("struct empty (no fields)", ("struct", []))

fees = ("list", [("struct", [("utf8", "a"), ("utf8", "1")]), ("struct", [("utf8", "b"), ("utf8", "22")])])
show("list<struct> [{a,1},{b,22}]", fees)
show("list<struct> [null struct]", ("list", [None]))
show("list<struct> []", ("list", []))

nested = ("list", [("struct", [("uint", 5), ("list", [("utf8", "x"), None])])])
show("list<struct{n: u64, tags: list<utf8>}> [{5,[x,null]}]", nested)

# A full Cosmos-like transactions row fragment: fee_amount and signer_infos.
row = encode_row([
    ("fee_amount", ("list", [fee])),
    ("signer_infos", ("list", [signer])),
])
print("row fee_amount+signer_infos:", row.hex())
print("row leaf sha256:", hashlib.sha256(b"\x00" + row).hexdigest())
