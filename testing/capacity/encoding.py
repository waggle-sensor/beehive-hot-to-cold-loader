"""Lossless LP encoding for the five field types supported by the archiver."""
import json
import math


def text(value):
    if not isinstance(value, str) or any(c in value for c in ("\n", "\r", "\x00")):
        raise ValueError("input cannot be represented faithfully in line protocol")
    return value


def measurement(value):
    value=text(value)
    if not value or value.startswith(("#", "_")):
        raise ValueError("invalid measurement")
    return value.replace(",", "\\,").replace(" ", "\\ ")


def tag(value):
    value=text(value)
    if not value:
        raise ValueError("empty tags cannot be round-tripped through Influx")
    return value.replace(",", "\\,").replace("=", "\\=").replace(" ", "\\ ")


def scalar(kind, encoded):
    value=json.loads(encoded)
    if kind=="string" and isinstance(value,str):
        # InfluxDB 2.7's quote-aware parser permits literal LF inside string
        # fields. Replacing it with backslash-n would change the stored value.
        # Identifier validation remains stricter; CR/NUL are not admitted here.
        if any(c in value for c in ("\r", "\x00")):
            raise ValueError("unsupported CR/NUL in string field")
        return '"'+value.replace("\\", "\\\\").replace('"','\\"')+'"'
    if kind=="bool" and isinstance(value,bool):
        return "true" if value else "false"
    if kind in ("int","uint") and isinstance(value,int) and not isinstance(value,bool):
        if kind=="int" and -(1<<63)<=value<(1<<63):return str(value)+"i"
        if kind=="uint" and 0<=value<(1<<64):return str(value)+"u"
    if kind=="float" and isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(float(value)):
        return repr(float(value))
    raise ValueError("unsupported or invalid scalar; no coercion/drop is permitted")


def line(row):
    stamp=row["time"]
    if isinstance(stamp,bool) or not isinstance(stamp,int) or not -(1<<63)<=stamp<(1<<63):
        raise ValueError("nanosecond int64 timestamp required")
    tags=json.loads(row["tags"])
    if not isinstance(tags,dict):raise ValueError("complete tag object required")
    parts=[measurement(row["name"])]
    for key,value in sorted(tags.items()):
        if key.startswith("_") or key in ("time",):raise ValueError("reserved tag key")
        parts.append(","+tag(key)+"="+tag(value))
    return ("".join(parts)+" value="+scalar(row["kind"],row["value"])+" "+str(stamp)+"\n").encode()


def batches(rows, row_limit=10000, byte_limit=4<<20):
    pending=bytearray();count=0
    for row in rows:
        encoded=line(row)
        if len(encoded)>byte_limit:raise ValueError("single point exceeds batch byte budget")
        if count and (count>=row_limit or len(pending)+len(encoded)>byte_limit):
            yield bytes(pending),count
            pending.clear();count=0
        pending.extend(encoded);count+=1
    if count:yield bytes(pending),count
