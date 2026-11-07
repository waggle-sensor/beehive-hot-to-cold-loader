"""Small real-service gate. Imports of container-only dependencies are lazy."""
import base64
from collections import Counter
import csv
from datetime import datetime, timezone
import gzip
import hashlib
import io
import importlib
from importlib.metadata import version as package_version
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
import uuid

START = 1740787200000000000
MID = START + 86400000000000
BOUNDS = ("2025-03-01T00:00:00Z", "2025-03-02T00:00:00Z", "2025-03-03T00:00:00Z")
ARROW = "application/vnd.apache.arrow.stream"
NDJSON = "application/x-ndjson"
REPORTS = Path("/reports")
BUCKET = "waggle"
TAGS = ("plugin", "vsn", "node", "host", "task", "job", "zone")


def record(stamp, name, value, **meta):
    return dict(time=stamp, name=name, value=value, meta=dict(node="n1", vsn="LIVE", **meta))


def expected(version):
    rows = [record(START+194, "live.temp", 11.7, plugin="fixture", sensor="temperature"),
            record(START+295, "live.text", "123", plugin="fixture", sensor="status"),
            record(MID+194, "live.hot", 22.2, plugin="fixture", sensor="temperature")]
    if version != "seed":
        rows.append(record(START+396, "live.buffered", "offline", sensor="buffer"))
    if version == "final":
        rows[0]["value"] = 11.9
        rows.append(record(START+497, "live.reconnected", "online", plugin="fixture", sensor="status"))
    return sorted(rows, key=lambda row: (row["time"], row["name"]))


def ns(text):
    match = re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(?:Z|\+00:00)", text)
    if not match:
        raise ValueError("expected a UTC ISO timestamp with at most nine fractional digits")
    delta = datetime.fromisoformat(match[1]).replace(tzinfo=timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days*86400+delta.seconds)*1_000_000_000 + int((match[2] or "").ljust(9, "0"))


def canonical(rows):
    return Counter(json.dumps(row, sort_keys=True, separators=(",", ":"), allow_nan=False) for row in rows)


def equal(rows, want):
    if canonical(rows) != canonical(want):
        raise AssertionError("record multiset differs: got=" + repr(rows) + " expected=" + repr(want))


def save(name, value):
    temporary = REPORTS / (name + ".pending")
    with temporary.open("w") as output:
        json.dump(value, output, indent=2, allow_nan=False)
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(REPORTS / (name + ".json"))


def load(name):
    return json.loads((REPORTS / (name + ".json")).read_text())


def call(url, data=None, *, method=None, headers=None, error_observer=None, timeout=90):
    if isinstance(data, (dict, list)):
        data = json.dumps(data).encode()
    request = Request(url, data=data, method=method, headers=headers or {})
    try:
        with urlopen(request, timeout=timeout) as response:
            raw = response.read()
            if response.headers.get("Content-Encoding", "").lower() == "gzip":
                raw = gzip.decompress(raw)
            return raw, response.headers
    except HTTPError as error:
        if error_observer is not None:
            # Opt-in, private diagnostics for callers that know their endpoint.
            # Keep the original HTTP failure even if diagnostics cannot be saved.
            try:
                error_observer(error)
            except Exception:
                pass
        # Never copy signed SQL/remote responses into the report.
        raise RuntimeError("HTTP " + str(error.code) + " from " + urlsplit(url).path) from None


def json_call(url, payload=None, token=None, method=None, headers=None):
    headers = {"Content-Type": "application/json", **(headers or {})}
    if token:
        headers["Authorization"] = "Bearer " + token
    raw, _ = call(url, payload, method=method, headers=headers)
    return json.loads(raw) if raw else None


def coord(path, payload=None, role="ADMIN"):
    return json_call("http://archiver:10001"+path, {"bucket": BUCKET, **(payload or {})}, os.environ[role+"_TOKEN"])


def wait(label, probe, timeout=240):
    deadline = time.monotonic()+timeout
    last = None
    while time.monotonic() < deadline:
        try:
            result = probe()
            if result:
                return result
        except (RuntimeError, URLError, OSError, sqlite3.Error) as error:
            last = type(error).__name__ + ": " + str(error)
        time.sleep(0.25)
    raise TimeoutError(label + "; last observation: " + str(last))


def db(query, values=()):
    with sqlite3.connect("file:/state/control.db?mode=ro", uri=True, timeout=3) as connection:
        connection.execute("PRAGMA query_only=ON")
        return connection.execute(query, values).fetchall()


def archive_job():
    rows = db("SELECT payload FROM jobs WHERE mode='archive' AND day='2025-03-01'")
    if not rows:
        return None
    job = json.loads(rows[0][0])
    if job.get("error"):
        raise AssertionError("archive job error; inspect private coordinator journal")
    return job


def inbox():
    return [dict(seq=row[0], record=json.loads(row[1]), done=row[2])
            for row in db("SELECT seq,payload,done FROM inbox ORDER BY seq")]


def source_rows(start=BOUNDS[0], end=BOUNDS[2]):
    payload = {"query": 'from(bucket: "waggle") |> range(start: time(v: '+json.dumps(start)+'), stop: time(v: '+json.dumps(end)+'))',
               "dialect": {"annotations": ["datatype", "group", "default"]}}
    raw, _ = call("http://influx:8086/api/v2/query?org=waggle", payload,
                  headers={"Authorization": "Token "+os.environ["INFLUX_TOKEN"], "Content-Type": "application/json", "Accept": "application/csv"})
    types, defaults, names, rows = [], [], [], []
    for fields in csv.reader(io.StringIO(raw.decode())):
        if not fields:
            continue
        if fields[0] == "#datatype": types = fields; continue
        if fields[0] == "#default": defaults = fields; continue
        if fields[0].startswith("#"): continue
        if "_time" in fields and "_value" in fields:
            names = fields; continue
        if not names or len(fields) != len(names):
            raise AssertionError("unexpected source CSV")
        fields = [value or (defaults[i] if i < len(defaults) else "") for i, value in enumerate(fields)]
        values = dict(zip(names, fields))
        if values.get("_field") != "value": raise AssertionError("unexpected source field")
        kind = types[names.index("_value")]
        value = values["_value"]
        if kind == "double": value = float(value)
        elif kind in ("long", "unsignedLong"): value = int(value)
        elif kind == "boolean": value = value == "true"
        elif kind != "string": raise AssertionError("unexpected source value type")
        meta = {key: value for key, value in values.items() if key not in ("", "result", "table") and not key.startswith("_") and value != ""}
        rows.append(dict(time=ns(values["_time"]), name=values["_measurement"], value=value, meta=meta))
    return rows


def amqp():
    import pika
    return pika.BlockingConnection(pika.ConnectionParameters(host="broker", credentials=pika.PlainCredentials("node-n1", os.environ["BROKER_PASSWORD"])))


def publish(rows, label):
    import pika
    import wagglemsg
    with amqp() as connection:
        channel = connection.channel()
        channel.confirm_delivery()
        for index, row in enumerate(rows):
            body = wagglemsg.dump(wagglemsg.Message(name=row["name"], value=row["value"], timestamp=row["time"], meta=row["meta"]))
            channel.basic_publish("archive.live", "measurements", body, mandatory=True,
                                  properties=pika.BasicProperties(user_id="node-n1", delivery_mode=2, message_id=os.environ["RUN_ID"]+"/"+label+"/"+str(index)))


def broker_queue():
    auth = base64.b64encode(("node-n1:"+os.environ["BROKER_PASSWORD"]).encode()).decode()
    return json_call("http://broker:15672/api/queues/%2F/archive.live", method="GET", headers={"Authorization": "Basic "+auth})


def queue_drained(queue, expected_acks):
    return (queue.get("messages_ready", -1) == 0
            and queue.get("messages_unacknowledged", -1) == 0
            and queue.get("message_stats", {}).get("ack", 0) >= expected_acks)


def drained(expected_acks):
    queue = broker_queue()
    return queue if queue_drained(queue, expected_acks) else None


def views():
    return set(row[0] for row in db("SELECT id FROM readers"))


def acquire_read(label):
    return coord("/v1/reads/acquire", {"id": os.environ["RUN_ID"]+"/"+label, "owner": os.environ["RUN_ID"]+"/probe"}, "READER")


def release_read(view):
    coord("/v1/reads/release", {"id": view["id"]}, "READER")


def ipc_rows(raw):
    import pyarrow as pa
    import pyarrow.ipc as ipc
    table = ipc.open_stream(raw).read_all()
    if table.schema.names != ["timestamp", "name", "value", "meta"]:
        raise AssertionError("public Arrow schema changed")
    stamp = table.schema.field("timestamp").type
    if not pa.types.is_timestamp(stamp) or stamp.unit != "ns":
        raise AssertionError("Arrow timestamps lost nanosecond precision")
    return [dict(time=stamp, name=name, value=json.loads(value), meta=json.loads(meta))
            for stamp, name, value, meta in zip(table["timestamp"].cast(pa.int64()).to_pylist(), table["name"].to_pylist(), table["value"].to_pylist(), table["meta"].to_pylist())]


def check_api(label, rows):
    import pandas as pd
    import sage_data_client
    checks = {}
    for scope, start, end in (("mixed", BOUNDS[0], BOUNDS[2]), ("day-one", BOUNDS[0], BOUNDS[1]), ("day-two", BOUNDS[1], BOUNDS[2])):
        want = [row for row in rows if ns(start) <= row["time"] < ns(end)]
        for accept in (NDJSON, ARROW):
            raw, headers = call("http://api:10000/api/v1/query", {"start": start, "end": end}, headers={"Content-Type": "application/json", "Accept": accept})
            if accept == ARROW and headers.get_content_type() != ARROW:
                raise AssertionError("API did not serve the requested format")
            if accept == ARROW:
                got = ipc_rows(raw)
                if headers.get("X-Arrow-Compression") != "zstd":
                    raise AssertionError("expected default Arrow zstd compression")
                suffix = ".arrow"
            else:
                if headers.get_content_type() == ARROW: raise AssertionError("NDJSON request returned Arrow")
                got = []
                for line in raw.splitlines():
                    row = json.loads(line)
                    if set(row) != {"timestamp", "name", "value", "meta"}: raise AssertionError("public NDJSON schema changed")
                    got.append(dict(time=ns(row["timestamp"]), name=row["name"], value=row["value"], meta=row["meta"]))
                suffix = ".ndjson"
            equal(got, want)
            (REPORTS / (label+"-"+scope+suffix)).write_bytes(raw)
            checks[scope+"-"+accept] = {"rows": len(got), "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(), "content_type": headers.get_content_type()}
        client_module = importlib.import_module("sage_data_client.query")
        original_open = client_module.urlopen
        received = []
        def observe_response(*args, **kwargs):
            response = original_open(*args, **kwargs)
            media = response.headers.get("Content-Type", "").split(";",1)[0].lower()
            received.append(media)
            if media != ARROW:
                response.close()
                raise AssertionError("default client request did not receive Arrow")
            return response
        client_module.urlopen = observe_response
        try:
            frame = sage_data_client.query(start=start, end=end, endpoint="http://api:10000/api/v1/query")
        finally:
            client_module.urlopen = original_open
        got = []
        for _, row in frame.iterrows():
            value = row["value"]
            if hasattr(value, "item"): value = value.item()
            meta = {key[5:]: value for key, value in row.items() if key.startswith("meta.") and not pd.isna(value)}
            got.append(dict(time=int(pd.Timestamp(row["timestamp"]).value), name=row["name"], value=value, meta=meta))
        equal(got, want)
        checks[scope+"-client"] = {"rows": len(got), "format": received[0]}
    return checks


def token(write=False):
    import jwt
    warehouse = os.environ["WAREHOUSE"]
    prefix = urlsplit(warehouse).path.split("/")[2]
    if not re.fullmatch("cold-storage-archive-live-"+os.environ["RUN_ID"], prefix):
        raise AssertionError("remote prefix is not this run's generated prefix")
    now = int(time.time())
    claims = dict(sub="sage-live-gate", aud="ANY", ver="scitoken:2.0", scope="read:/"+prefix+(" write:/"+prefix if write else ""),
                  iss="https://sagecontinuum.org", iat=now-60, nbf=now-60, exp=now+900, jti=uuid.uuid4().hex)
    return jwt.encode(claims, Path(os.environ["PELICAN_KEY_PATH"]).read_bytes(), algorithm="ES256", headers={"kid":"3cfa"})


def sqlquote(value):
    return "'"+value.replace("'", "''")+"'"


def remote_rows(metadata):
    warehouse = os.environ["WAREHOUSE"]
    if not metadata.startswith(warehouse+"/sage/obs/metadata/") or not metadata.endswith(".metadata.json"):
        raise AssertionError("snapshot outside isolated managed warehouse")
    keys = ",".join(sqlquote(tag) for tag in TAGS)
    full_meta = "map_concat(meta,map_from_entries(list_filter(list_zip(["+keys+"],["+",".join(TAGS)+"]),lambda x: x[2] IS NOT NULL)))"
    sql = "SET TimeZone='UTC'; SET memory_limit='256MB'; LOAD httpfs; LOAD iceberg; CREATE SECRET live_read (TYPE http,SCOPE "+sqlquote(warehouse)+",BEARER_TOKEN "+sqlquote(token())+"); SELECT epoch_ns(time) AS time,name,to_json(value)::VARCHAR AS value,to_json("+full_meta+")::VARCHAR AS meta FROM iceberg_scan("+sqlquote(metadata)+");"
    result = subprocess.run(["duckdb", "-batch", "-bail", "-json"], input=sql, text=True, capture_output=True, timeout=180)
    if result.returncode:
        raise RuntimeError("pinned remote DuckDB read failed; SQL/stderr withheld because it contains a credential")
    decoder = json.JSONDecoder()
    text = result.stdout.lstrip()
    last = None
    while text:
        last, offset = decoder.raw_decode(text)
        text = text[offset:].lstrip()
    if not isinstance(last, list): raise AssertionError("missing pinned scan result")
    return [dict(time=int(row["time"]), name=row["name"], value=json.loads(row["value"]), meta=json.loads(row["meta"])) for row in last]


def bootstrap(declare_broker=True):
    version = subprocess.run(["duckdb", "-version"], capture_output=True, text=True, check=True).stdout
    if not re.search(r"\bv2\.0\.\d", version): raise AssertionError("DuckDB 2.0.x required")
    warehouse = os.environ["WAREHOUSE"]
    auth = {"Authorization": "Bearer "+token(write=True)}
    # A new UUID is allocated for every run. Never reuse a published warehouse.
    try:
        with urlopen(Request(warehouse+"/sage/obs/metadata/version-hint.text", headers=auth), timeout=60):
            raise AssertionError("generated prefix already has a published warehouse")
    except HTTPError as error:
        if error.code != 404: raise RuntimeError("new-prefix preflight returned HTTP "+str(error.code)) from None
    # Keep the origin marker outside sage/obs. A file there creates the managed
    # table collection, and a new catalog refuses to adopt an occupied root.
    marker = warehouse+"/validation/live-gate.json"
    data = json.dumps({"run_id":os.environ["RUN_ID"]}).encode()
    call(marker, data, method="PUT", headers={**auth,"Content-Type":"application/json"})
    readback, _ = call(marker, headers=auth)
    if readback != data: raise AssertionError("real origin marker readback differed")
    influx_headers = {"Authorization": "Token "+os.environ["INFLUX_TOKEN"]}
    def discover_bucket():
        buckets=json_call("http://influx:8086/api/v2/buckets?name=waggle", method="GET", headers=influx_headers)
        selected=[bucket for bucket in buckets["buckets"] if bucket["name"]==BUCKET]
        if not selected: return None
        if len(selected)!=1 or any(rule.get("everySeconds",0)!=0 for rule in selected[0].get("retentionRules",[])): raise AssertionError("expected one unlimited-retention fixture bucket")
        return selected[0]
    selected=wait("fresh Influx setup", discover_bucket)
    def declare_topology():
        try:
            with amqp() as connection:
                channel=connection.channel()
                channel.exchange_declare("archive.live", exchange_type="topic", durable=True)
                channel.queue_declare("archive.live", durable=True)
                channel.queue_bind("archive.live", "archive.live", "#")
            return True
        except Exception as error:
            if type(error).__name__.startswith("AMQP"):
                raise OSError(type(error).__name__) from error
            raise
    if declare_broker:
        wait("private broker topology", declare_topology)
    return {"bucket_id":selected["id"], "warehouse":warehouse, "duckdb":version.strip(), "origin_marker_readback":True,
            "dependencies":{name:package_version(name) for name in ("sage-data-client","pandas","pyarrow","pika","pywagglemsg","influxdb-client","PyJWT")}}


def ready():
    status=wait("coordinator readiness", lambda: coord("/v1/status"))
    if db("SELECT warehouse FROM config WHERE id=1")[0][0] != os.environ["WAREHOUSE"]: raise AssertionError("wrong coordinator warehouse")
    return status


def seed():
    publish(expected("seed"), "seed")
    wait("seed broker acknowledgement", lambda: drained(3))
    wait("seed visible in Influx", lambda: canonical(source_rows()) == canonical(expected("seed")))
    checks=wait("managed API startup/hot read", lambda: check_api("seed", expected("seed")))
    permit=os.environ["RUN_ID"]+"/held-writer"
    granted=coord("/v1/writes/acquire", {"id":permit,"owner":os.environ["RUN_ID"]+"/probe", "days":["2025-03-01"]}, "LOADER")
    if granted["hot_days"] != ["2025-03-01"]: raise AssertionError("initial day was not hot")
    hot=acquire_read("held-hot-reader")
    if hot["frontier"]!=BOUNDS[0]: raise AssertionError("initial frontier differs")
    save("pins", {"writer":permit, "hot":hot})
    return {"api":checks,"broker":broker_queue(),"held_view":hot}


def fenced():
    wait("fenced archive job", lambda: (archive_job() or {}).get("state")=="FENCING")
    publish([row for row in expected("fenced") if row["name"]=="live.buffered"], "fenced")
    wait("durable fenced inbox receipt", lambda: len(inbox())==1 and inbox()[0]["done"]==0)
    wait("late broker acknowledgement after receipt", lambda: drained(4))
    equal(source_rows(),expected("seed"))
    if archive_job()["state"]!="FENCING": raise AssertionError("writer permit did not hold fence")
    return {"inbox":inbox(),"broker":broker_queue(),"status":coord("/v1/status")}


def recover_and_release():
    pins=load("pins")
    if len(inbox())!=1 or inbox()[0]["done"]!=0: raise AssertionError("acknowledged receipt lost across restart")
    if db("SELECT state FROM permits WHERE id=?",(pins["writer"],)) != [("active",)]: raise AssertionError("write permit did not survive restart")
    if pins["hot"]["id"] not in views() or archive_job()["state"]!="FENCING": raise AssertionError("read/fence state lost across restart")
    coord("/v1/writes/release", {"id":pins["writer"], "owner":os.environ["RUN_ID"]+"/probe"}, "LOADER")
    return {"durable_receipt_and_grants_survived":True}


def archive_and_pin():
    wait("verified archive routed", lambda: (archive_job() or {}).get("state")=="ROUTED")
    pins=load("pins")
    pins["cold"]=acquire_read("held-cold-reader")
    # The durable archive journal records the first snapshot even if container
    # startup allowed the queued late job to route before this probe acquired a
    # view. Check that first snapshot independently; don't depend on a race.
    pins["initial_metadata"]=archive_job()["metadata"]
    initial=[row for row in expected("seed") if row["time"]<MID]
    equal(remote_rows(pins["initial_metadata"]),initial)
    pins["cold_rows"]=remote_rows(pins["cold"]["metadata"])
    fenced_rows=[row for row in expected("fenced") if row["time"]<MID]
    if canonical(pins["cold_rows"]) not in (canonical(initial),canonical(fenced_rows)):
        raise AssertionError("cold grant did not select a valid pre-correction snapshot")
    save("pins",pins)
    if pins["cold"]["frontier"]!=BOUNDS[1]: raise AssertionError("frontier not advanced")
    return {"pinned_view":pins["cold"],"initial_metadata":pins["initial_metadata"],"initial_remote_rows":len(initial),"job":archive_job()}


def settled(version):
    state=coord("/v1/status")
    if state["late_pending"]!=0 or db("SELECT count(*) FROM jobs WHERE mode='late' AND state!='COMPLETE'")[0][0]: return None
    if state["jobs_failed"] or state["quarantined"]: raise AssertionError("unexpected failed job/quarantine")
    rows=remote_rows(state["metadata"])
    want=[row for row in expected(version) if row["time"]<MID]
    return state if canonical(rows)==canonical(want) else None


def fenced_late_published():
    status=wait("fenced late correction published",lambda:settled("fenced"))
    equal(source_rows(),expected("seed"))
    return {"status":status,"api":check_api("fenced-late",expected("fenced")),"inbox":inbox()}


def publish_redelivery():
    rows=[row for row in expected("final") if row["name"] in ("live.temp","live.reconnected")]
    publish(rows[:1],"late-reconnect-correction")
    wait("message queued while loader stopped",lambda:broker_queue().get("messages_ready")==1)
    with amqp() as connection:
        channel=connection.channel()
        method, _, body=channel.basic_get("archive.live",auto_ack=False)
        if method is None: raise AssertionError("no delivery to requeue")
        channel.close()  # Real unacknowledged delivery is requeued by RabbitMQ.
        channel=connection.channel()
        def get_requeued():
            delivery=channel.basic_get("archive.live",auto_ack=False)
            return delivery if delivery[0] is not None else None
        method, _, replay=wait("broker requeue visibility",get_requeued,timeout=30)
        if not method.redelivered or replay!=body: raise AssertionError("broker did not redeliver the same unacked message")
        channel.close()  # The workspace loader must now consume and ack it.
    publish(rows[1:],"late-reconnect-new")
    wait("both late messages ready for loader",lambda:broker_queue().get("messages_ready")==2)
    return {"real_broker_redelivery_observed":True,"broker":broker_queue()}


def late_corrected():
    wait("late delivery acknowledged",lambda:drained(6))
    status=wait("late correction committed and remotely verified",lambda:settled("final"))
    pins=load("pins")
    equal(remote_rows(pins["cold"]["metadata"]),pins["cold_rows"])
    equal(remote_rows(pins["initial_metadata"]),[row for row in expected("seed") if row["time"]<MID])
    equal(source_rows(),expected("seed"))
    checks=check_api("corrected",expected("final"))
    wait("all API grants released",lambda:views()=={pins["hot"]["id"],pins["cold"]["id"]})
    return {"status":status,"api":checks,"old_snapshot_unchanged":True,"inbox":inbox(),"broker":broker_queue(),
            "warehouse_contract":warehouse_contract(status["metadata"]),"verification_journal":verification_journal(before_delete=True)}


def deletion_blocked():
    pins=load("pins")
    # Observe across scheduler ticks; a ready status alone proves no attempted
    # deletion. The host explicitly started this process with deletion enabled.
    began=time.monotonic()
    while time.monotonic()-began<3:
        if archive_job()["state"]!="ROUTED" or pins["hot"]["id"] not in views(): raise AssertionError("old read did not block deletion")
        equal(source_rows(),expected("seed"))
        time.sleep(0.25)
    return {"old_hot_grant_blocks_deletion":True,"status":coord("/v1/status"),"job":archive_job()}


def release_and_finish():
    pins=load("pins")
    release_read(pins["hot"])
    wait("verified fixture deletion completed",lambda:archive_job()["state"]=="COMPLETE")
    equal(source_rows(),[row for row in expected("seed") if row["time"]>=MID])
    checks=check_api("after-delete",expected("final"))
    equal(remote_rows(pins["cold"]["metadata"]),pins["cold_rows"])
    equal(remote_rows(pins["initial_metadata"]),[row for row in expected("seed") if row["time"]<MID])
    release_read(pins["cold"])
    status=wait("readers/writers/late work drained",lambda: clean_status())
    return {"status":status,"api":checks,"job":archive_job(),"old_snapshot_still_readable":True,"broker":broker_queue(),
            "warehouse_contract":warehouse_contract(status["metadata"]),"verification_journal":verification_journal()}


def warehouse_contract(metadata):
    root=os.environ["WAREHOUSE"]
    if not metadata.startswith(root+"/sage/obs/metadata/"):
        raise AssertionError("metadata outside this run")
    body=json_call(metadata,method="GET",headers={"Authorization":"Bearer "+token()})
    if body["format-version"]!=3 or body["location"]!=root+"/sage/obs":
        raise AssertionError("wrong Iceberg format/location")
    current=next(schema for schema in body["schemas"] if schema["schema-id"]==body["current-schema-id"])
    fields={field["name"]:field for field in current["fields"]}
    if fields["time"]["type"]!="timestamptz_ns" or fields["time"]["id"]!=1:
        raise AssertionError("stored nanosecond type changed")
    if fields["value"]["type"]!="variant" or fields["value"]["id"]!=11:
        raise AssertionError("stored VARIANT type/id changed")
    if fields["meta"]["type"]["type"]!="map" or fields["meta"]["id"]!=10:
        raise AssertionError("stored MAP metadata changed")
    props=body["properties"]
    for key,value in {"sage.layout":"plugin-vsn-time","sage.layout.version":"bounded-plugin-v1","sage.batch.order":"time","sage.row-group.rows":"262144"}.items():
        if props.get(key)!=value: raise AssertionError("warehouse layout property changed: "+key)
    spec=next(spec for spec in body["partition-specs"] if spec["spec-id"]==body["default-spec-id"])
    if not any(field["source-id"]==1 and field["transform"]=="day" for field in spec["fields"]):
        raise AssertionError("warehouse is not day partitioned")
    save("published-metadata",body)
    return {"format":3,"time_type":fields["time"]["type"],"value_type":fields["value"]["type"],"meta_type":"map","layout":props["sage.layout.version"]}


def validate_normalization_policy(stats):
    if stats.get('threads')!=16 or stats.get('merge_fan_in')!=16 or stats.get('merge_concurrency')!=1:
        raise AssertionError('normalization must use sixteen threads/fan-in and one merge worker')
    leaves=stats.get('partitions',0)
    if leaves<1 or stats.get('max_merge_fan_in')!=min(leaves,16):
        raise AssertionError('actual normalization fan-in differs from policy')
    passes=1
    while leaves>16:
        leaves=(leaves+15)//16
        passes+=1
    if stats.get('merge_passes')!=passes:
        raise AssertionError('normalization merge pass count differs from policy')


def verification_journal(before_delete=False):
    audits=[]
    for raw, in db("SELECT payload FROM jobs ORDER BY rowid"):
        job=json.loads(raw)
        if job["state"]!="COMPLETE" and not (before_delete and job["mode"]=="archive" and job["state"]=="ROUTED"):
            raise AssertionError("unexpected unfinished job at verification audit")
        work=Path(job["work"])
        if work!=Path("/state/jobs")/job["id"]: raise AssertionError("unexpected job staging path")
        normalization=None
        if job["mode"]=="archive":
            normalization=json.loads((work/"export-dedup.json").read_text())
            validate_normalization_policy(normalization)
        stages=["COMMITTED"]+(["DELETING"] if job["mode"]=="archive" and not before_delete else [])
        for stage in stages:
            report=json.loads((work/("verification-"+stage+".json")).read_text())
            if report.get("full_multiset_equal") is not True or report["metadata"]!=job["metadata"] or not report["file_sha256"]:
                raise AssertionError("missing full remote verification/hash evidence")
            if report["source_bucket_id"]!=load("bootstrap")["bucket_id"]:
                raise AssertionError("verification used a different source bucket")
            audits.append({"job":job["id"],"mode":job["mode"],"stage":stage,"report":report,"normalization":normalization})
    return audits


def clean_status():
    state=coord("/v1/status")
    return state if all(state[key]==0 for key in ("writers","readers","late_pending","jobs_pending","deletions_pending","jobs_failed","quarantined")) else None


def main():
    stage=sys.argv[1]
    stages={name.replace("_","-"):value for name,value in globals().copy().items() if name in (
        "bootstrap","ready","seed","fenced","recover_and_release","archive_and_pin","fenced_late_published",
        "publish_redelivery","late_corrected","deletion_blocked","release_and_finish")}
    try:
        result=stages[stage]()
        save(stage,{"passed":True,**result})
        print(stage+": passed",flush=True)
    except BaseException as error:
        message=str(error)
        for key in ("INFLUX_TOKEN","BROKER_PASSWORD","LOADER_TOKEN","READER_TOKEN","ADMIN_TOKEN"):
            if os.getenv(key): message=message.replace(os.environ[key],"<redacted>")
        save(stage,{"passed":False,"error_type":type(error).__name__,"error":message})
        print(stage+": failed; inspect its private JSON report",file=sys.stderr)
        raise SystemExit(1) from None


if __name__=="__main__":
    main()
