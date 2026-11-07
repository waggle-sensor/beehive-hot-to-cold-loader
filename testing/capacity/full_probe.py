"""Bounded historical-day clone and full-multiset archival verification."""
from datetime import date,datetime,timedelta,timezone
from decimal import Decimal
from collections import Counter
import csv
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import resource
import shutil
import subprocess
import sys
import time
from urllib.parse import urlsplit
import uuid

import probe as base
from encoding import batches

REPORTS=Path("/reports")
SOURCE_FILE=REPORTS/"source.parquet"


def bounds():
    day=date.fromisoformat(os.environ["SOURCE_DAY"])
    return day.isoformat()+"T00:00:00Z",(day+timedelta(days=1)).isoformat()+"T00:00:00Z",(day+timedelta(days=2)).isoformat()+"T00:00:00Z"


def auth(warehouse):
    import jwt
    u=urlsplit(warehouse)
    if (u.scheme!="https" or not u.netloc or u.query or u.fragment or u.username or u.password
        or not re.fullmatch(r"/sage/cold-storage-[A-Za-z0-9_-]+/warehouse",u.path)):
        raise ValueError("invalid read-only warehouse")
    now=int(time.time());prefix=u.path.split("/")[2]
    return jwt.encode(dict(sub="sage-capacity-reader",aud="ANY",ver="scitoken:2.0",scope="read:/"+prefix,
        iss="https://sagecontinuum.org",iat=now-60,nbf=now-60,exp=now+int(os.environ["DAY_TIMEOUT"])+60,jti=uuid.uuid4().hex),
        Path(os.environ["PELICAN_KEY_PATH"]).read_bytes(),algorithm="ES256",headers={"kid":"3cfa"})


def duck(sql, warehouses=()):
    free=shutil.disk_usage(REPORTS).free
    if free<=8*(1<<30):raise RuntimeError('insufficient staging disk: 8 GiB reserve required')
    spill=min(16*(1<<30),free-8*(1<<30))
    setup="SET TimeZone='UTC';SET threads=2;SET memory_limit="+base.sqlquote(os.environ["NATIVE_MEMORY"])+";SET temp_directory='/reports/native-spill';SET max_temp_directory_size="+base.sqlquote(str(spill)+'B')+";LOAD httpfs;LOAD iceberg;"
    credentials=[]
    for index,warehouse in enumerate(warehouses):
        credential=auth(warehouse);credentials.append(credential)
        setup+="CREATE SECRET read_"+str(index)+" (TYPE http,SCOPE "+base.sqlquote(warehouse)+",BEARER_TOKEN "+base.sqlquote(credential)+");"
    command=subprocess.run(["duckdb","-batch","-bail","-json"],input=setup+sql,text=True,capture_output=True,timeout=int(os.environ["DAY_TIMEOUT"]))
    if command.returncode:
        diagnostic=command.stderr
        for credential in credentials:diagnostic=diagnostic.replace(credential,"<redacted>")
        diagnostic=re.sub(r"eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+","<redacted>",diagnostic)
        if any(word in diagnostic.lower() for word in ('secret','token','bearer','authz=')):
            diagnostic='credential-bearing diagnostic withheld'
        base.save('native-error',{'stderr':diagnostic[:8000],'returncode':command.returncode})
        raise RuntimeError("native capacity SQL failed; inspect private native-error.json and resources")
    decoder=json.JSONDecoder();text=command.stdout.lstrip();last=[]
    while text:last,offset=decoder.raw_decode(text);text=text[offset:].lstrip()
    return last


def tag_expr():
    keys=",".join(base.sqlquote(tag) for tag in base.TAGS)
    return "map_from_entries(list_sort(map_entries(map_concat(coalesce(meta,MAP{}::MAP(VARCHAR,VARCHAR)),map_from_entries(list_filter(list_zip(["+keys+"],["+",".join(base.TAGS)+"]),lambda x: x[2] IS NOT NULL))))))"


def day_scan(metadata):
    start,end,_=bounds()
    return "SELECT * FROM iceberg_scan("+base.sqlquote(metadata)+") WHERE time>=TIMESTAMPTZ_NS "+base.sqlquote(start)+" AND time<TIMESTAMPTZ_NS "+base.sqlquote(end)


def canonical_scan(metadata):
    return "SELECT epoch_ns(time) AS time,name,to_json("+tag_expr()+")::VARCHAR AS tags,variant_typeof(value)::VARCHAR AS kind,to_json(value)::VARCHAR AS value FROM ("+day_scan(metadata)+")"


def source_canonical():
    return "SELECT time,name,tags,native_kind AS kind,value FROM read_parquet('/reports/source.parquet')"


def canonical_equal(left,right,warehouses):
    """Hash routes records; exact two-way EXCEPT ALL proves equality per shard."""
    import pyarrow.parquet as pq
    counts=duck('SELECT (SELECT count(*) FROM ('+left+')) AS a,(SELECT count(*) FROM ('+right+')) AS b;',warehouses)
    if len(counts)!=1 or set(counts[0])!={'a','b'}:raise AssertionError('missing comparison counts')
    a,b=counts[0]['a'],counts[0]['b']
    if a<0 or b<0 or a!=b:raise AssertionError('pinned original/full archived row counts differ')
    root=REPORTS/('compare-'+uuid.uuid4().hex);root.mkdir()
    columns='time,name,tags,kind,value';comparisons=[];serial=0
    def copy(source,path,bits):
        return 'COPY (SELECT *, (hash(time,name,tags,kind,value) >> '+str(bits)+')%16 AS shard FROM ('+source+')) TO '+base.sqlquote(str(path))+" (FORMAT parquet,PARTITION_BY(shard),COMPRESSION zstd,COMPRESSION_LEVEL 3,ROW_GROUP_SIZE 65536);"
    def inventory(path):
        groups={};total=0
        if not path.exists():return groups,total
        for entry in path.iterdir():
            if not entry.is_dir() or not re.fullmatch(r'shard=\d+',entry.name):raise AssertionError('unexpected comparison partition')
            shard=int(entry.name.split('=')[1])
            if not 0<=shard<16:raise AssertionError('comparison partition outside bucket range')
            files=sorted(entry.glob('*.parquet'));rows=0
            for file in files:
                with pq.ParquetFile(file) as reader:rows+=reader.metadata.num_rows
            groups[shard]=(files,rows);total+=rows
        return groups,total
    def scan(files):
        if not files:return 'SELECT NULL::BIGINT AS time,NULL::VARCHAR AS name,NULL::VARCHAR AS tags,NULL::VARCHAR AS kind,NULL::VARCHAR AS value WHERE false'
        return 'SELECT '+columns+' FROM read_parquet(['+','.join(base.sqlquote(str(p)) for p in files)+'],hive_partitioning=false)'
    def stage(left,right,session,bits,an,bn):
        nonlocal serial
        serial+=1;part=root/str(serial);part.mkdir()
        duck('SET partitioned_write_max_open_files=32;SET partitioned_write_flush_threshold=65536;SET preserve_insertion_order=false;'+copy(left,part/'a',bits)+copy(right,part/'b',bits),session)
        ap,ac=inventory(part/'a');bp,bc=inventory(part/'b')
        if ac!=an or bc!=bn:raise AssertionError('comparison staging row counts changed')
        for shard in sorted(ap.keys()|bp.keys()):
            af,ac=ap.get(shard,([],0));bf,bc=bp.get(shard,([],0))
            if ac!=bc:raise AssertionError('pinned original/full archived partition counts differ')
            if ac>1000000:
                if bits+4>=64:raise RuntimeError('identical-hash comparison partition exceeds 1000000-row capacity; equality not accepted')
                stage(scan(af),scan(bf),(),bits+4,ac,bc)
            elif ac:
                comparisons.append('WITH a AS ('+scan(af)+'),b AS ('+scan(bf)+") SELECT CASE WHEN count(*)=0 THEN true ELSE error('pinned original/full archived multiset differs') END AS equal FROM ((SELECT * FROM a EXCEPT ALL SELECT * FROM b) UNION ALL (SELECT * FROM b EXCEPT ALL SELECT * FROM a));")
    try:
        stage(left,right,warehouses,0,a,b)
        result=duck(''.join(comparisons)+'SELECT true AS all_partitions_equal;')
        if result!=[{'all_partitions_equal':True}]:raise AssertionError('missing comparison completion result')
        return {'rows':a,'partitions':len(comparisons),'partition_row_limit':1000000,'exact_multiset_equal':True}
    finally:
        shutil.rmtree(root)


def job():
    rows=base.db("SELECT payload FROM jobs WHERE mode='archive' AND day=?",(os.environ["SOURCE_DAY"],))
    return json.loads(rows[0][0]) if rows else None


def wait_job(state):
    unavailable=0;last_saved=None
    def ready():
        nonlocal unavailable,last_saved
        try:
            base.coord('/v1/status');unavailable=0
        except (RuntimeError,OSError):
            unavailable+=1
            if unavailable>=8:raise AssertionError('coordinator disappeared during capacity stage; inspect resources/retained container OOM state')
            return None
        value=job()
        encoded=json.dumps(value,sort_keys=True)
        if value and encoded!=last_saved:
            base.save("job-latest",value);last_saved=encoded
        return value if value and value["state"]==state else None
    return base.wait("full-day job "+state,ready,timeout=int(os.environ["DAY_TIMEOUT"]))


def vm_available():
    fields={line.split(":")[0]:int(line.split()[1])*1024 for line in Path('/proc/meminfo').read_text().splitlines() if ':' in line and line.split()[1].isdigit()}
    available=fields["MemAvailable"]
    if available<int(os.environ["MIN_AVAILABLE_GIB"])*(1<<30):raise RuntimeError("insufficient VM MemAvailable for declared capacity budget; make room before a fresh run")
    return available


def bootstrap():
    available=vm_available()
    result=base.bootstrap(declare_broker=False)
    result["vm_mem_available_bytes"]=available
    return result


def ready():return base.ready()


def strings():
    """Check the real HTTP parser before scanning/seeding the historical day.

    The extra bucket belongs only to this fresh fixture. Keep it for inspection;
    it is outside the coordinated waggle bucket and is never archived/deleted.
    """
    headers={"Authorization":"Token "+os.environ["INFLUX_TOKEN"]}
    buckets=base.json_call("http://influx:8086/api/v2/buckets?name=waggle",method="GET",headers=headers)
    selected=[b for b in buckets["buckets"] if b["name"]=="waggle"]
    if len(selected)!=1:raise AssertionError("expected one private source bucket")
    bucket=base.json_call("http://influx:8086/api/v2/buckets",
        {"name":"capacity-strings-"+os.environ["RUN_ID"],"orgID":selected[0]["orgID"],"retentionRules":[]},headers=headers)
    values=["first\nsecond","\nleading","trailing\n","a\n\nb",r"literal\ntext",
        'quote " and slash \\\nnext',"a\nforged,node=n1 value=99i 1740787200000000000"]
    start,end,_=bounds();stamp=base.ns(start)+194
    records=[dict(time=stamp+i,name="capacity.strings",tags='{"node":"capacity-fixture"}',kind="string",value=json.dumps(v)) for i,v in enumerate(values)]
    for payload,_ in batches(records):
        base.call("http://influx:8086/api/v2/write?org=waggle&bucket="+bucket["id"]+"&precision=ns",gzip.compress(payload,compresslevel=1),
            headers={**headers,"Content-Type":"text/plain","Content-Encoding":"gzip"})
    query='from(bucket:'+json.dumps(bucket["name"])+') |> range(start:time(v:'+json.dumps(start)+'), stop:time(v:'+json.dumps(end)+'))'
    raw,_=base.call("http://influx:8086/api/v2/query?org=waggle",{"query":query,"dialect":{"annotations":["datatype","group","default"],"dateTimeFormat":"RFC3339Nano"}},
        headers={**headers,"Content-Type":"application/json","Accept":"application/csv"})
    names=[];got=[]
    for fields in csv.reader(io.StringIO(raw.decode(),newline="")):
        if not fields or fields[0].startswith('#'):continue
        if '_value' in fields and '_time' in fields:names=fields;continue
        if not names or len(fields)!=len(names):raise AssertionError("malformed string probe CSV")
        row=dict(zip(names,fields))
        # Influx 2.7.7 annotated CSV is CRLF and emits a stored LF as CRLF inside the quotes.
        value=row['_value'].replace('\r\n','\n')
        got.append((base.ns(row['_time']),row['_measurement'],row['_field'],row['node'],value))
    want=[(stamp+i,"capacity.strings","value","capacity-fixture",v) for i,v in enumerate(values)]
    if Counter(got)!=Counter(want):raise AssertionError("literal newline/string HTTP round-trip changed records")
    return {"rows":len(values),"literal_lf_preserved":True,"literal_backslash_n_distinct":True,"nanoseconds_preserved":True,
        "bucket_id":bucket["id"],"csv_sha256":hashlib.sha256(raw).hexdigest(),"scope":"small separate fixture bucket; not full-day capacity evidence"}


def source():
    warehouse=os.environ["SOURCE_WAREHOUSE"]
    if warehouse==os.environ["WAREHOUSE"]:raise ValueError("source must remain separate/read-only")
    raw,_=base.call(warehouse+"/sage/obs/metadata/version-hint.text",headers={"Authorization":"Bearer "+auth(warehouse)})
    stem=raw.decode().strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]+",stem):raise ValueError("unsafe source version hint")
    metadata=warehouse+"/sage/obs/metadata/"+stem+".metadata.json"
    # Native type labels are calibrated against DuckDB itself, never guessed.
    cases=" ".join("WHEN variant_typeof(value)=variant_typeof("+literal+"::VARIANT) THEN "+base.sqlquote(kind)
        for literal,kind in (("0::DOUBLE","float"),("0::BIGINT","int"),("0::UBIGINT","uint"),("false","bool"),("''::VARCHAR","string")))
    projection="SELECT epoch_ns(time) AS time,name,to_json("+tag_expr()+")::VARCHAR AS tags,CASE "+cases+" ELSE error('unsupported VARIANT scalar; no conversion permitted') END AS kind,variant_typeof(value)::VARCHAR AS native_kind,to_json(value)::VARCHAR AS value FROM ("+day_scan(metadata)+")"
    pending=str(SOURCE_FILE)+".pending"
    duck("COPY ("+projection+") TO "+base.sqlquote(pending)+" (FORMAT parquet,COMPRESSION zstd,ROW_GROUP_SIZE 122880);",[warehouse])
    Path(pending).replace(SOURCE_FILE)
    stats=duck("SELECT count(*) AS rows,min(time) AS min_time,max(time) AS max_time,sum(octet_length(encode(to_json(struct_pack(time:=time,name:=name,tags:=tags::JSON,kind:=kind,value:=CASE WHEN kind='string' THEN json_extract_string(value,'$') ELSE value END,seq:=999999999999))::VARCHAR))+1)::BIGINT AS estimated_raw_ndjson_bytes FROM read_parquet('/reports/source.parquet');")[0]
    if stats["rows"]<1:raise AssertionError("source day is empty")
    kinds=duck("SELECT kind,count(*) AS rows FROM read_parquet('/reports/source.parquet') GROUP BY kind;")
    free=shutil.disk_usage(REPORTS).free
    reserve=max(64*(1<<30),int(os.environ["MIN_FREE_GIB"])*(1<<30),32*SOURCE_FILE.stat().st_size+32*(1<<30))
    result={"warehouse":warehouse,"metadata":metadata,"day":os.environ["SOURCE_DAY"],"stats":stats,"kinds":kinds,
        "expected_rows":int(os.environ["EXPECTED_ROWS"]),"primitive_parquet_bytes":SOURCE_FILE.stat().st_size,"free_bytes":free,"required_free_bytes_estimate":reserve,
        "disk_estimate_scope":"compressed staging heuristic: max(64 GiB, configured floor, 32 reference Parquet copies + 32 GiB); not a proven bound"}
    base.save("source-pinned",result)
    if result["expected_rows"] and stats["rows"]!=result["expected_rows"]:raise AssertionError("source row count differs from audited expectation; inspect source-pinned.json")
    if free<reserve:raise RuntimeError("insufficient free staging disk: inspect source-pinned.json; use a fresh run on a larger isolated volume")
    return result


def rows():
    import pyarrow.parquet as pq
    file=pq.ParquetFile(SOURCE_FILE)
    for batch in file.iter_batches(batch_size=10000,use_threads=False):
        yield from batch.to_pylist()


def redact_diagnostic(message):
    for key,value in os.environ.items():
        if key.endswith('_TOKEN') and value:message=message.replace(value,'<redacted>')
    return re.sub(r'eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+','<redacted-jwt>',message)


def influx_call(endpoint,data,*,headers):
    # Keep response diagnostics only from the private fixture's Influx endpoint,
    # never from signed DuckDB SQL or a remote storage origin.
    if endpoint not in ('/api/v2/write?org=waggle&bucket=waggle&precision=ns','/api/v2/query?org=waggle'):
        raise ValueError('unexpected capacity Influx endpoint')
    def observe(error):
        body=error.read(8193)
        base.save('influx-http-error',{'status':error.code,'endpoint':endpoint.split('?')[0],
            'body':redact_diagnostic(body[:8192].decode('utf-8',errors='replace')),'body_truncated':len(body)>8192,
            'scope':'bounded private Influx error response; no request payload/headers retained'})
    return base.call('http://influx:8086'+endpoint,data,headers=headers,error_observer=observe)


def flux_count_window(start,end):
    query='from(bucket:"waggle") |> range(start:time(v:'+json.dumps(start)+'), stop:time(v:'+json.dumps(end)+')) |> count(column:"_value") |> group() |> sum(column:"_value")'
    raw,_=influx_call('/api/v2/query?org=waggle',{"query":query,"dialect":{"annotations":["datatype","group","default"]}},
        headers={"Authorization":"Token "+os.environ["INFLUX_TOKEN"],"Content-Type":"application/json","Accept":"application/csv"})
    names=[];counts=[]
    for fields in csv.reader(io.StringIO(raw.decode())):
        if not fields or fields[0].startswith('#'):continue
        if len(fields)>1 and fields[1]=='error':raise AssertionError('Influx count returned an error table')
        if '_value' in fields:names=fields;continue
        if not names or len(fields)!=len(names):raise AssertionError('malformed Influx count CSV')
        count=int(fields[names.index('_value')])
        if count<0:raise AssertionError('negative Influx count')
        counts.append(count)
    if len(counts)>1:raise AssertionError("source count did not collapse to one result")
    return sum(counts)


def flux_count(start,end,progress=None):
    # Counts remain exact over the full half-open range, but no single query
    # needs a whole day's per-series execution state. This is preparation/
    # verification work, separate from the measured archiver stages.
    low=base.ns(start);high=base.ns(end);total=0;windows=0
    if high<=low:raise ValueError('positive Flux count range required')
    def stamp(ns):
        return datetime.fromtimestamp(ns//1_000_000_000,timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')+f'.{ns%1_000_000_000:09d}Z'
    while low<high:
        stop=min(low+300*1_000_000_000,high)
        total+=flux_count_window(stamp(low),stamp(stop));windows+=1
        if progress:progress(total,windows,stamp(stop))
        low=stop
    return total


def sentinel_name():return "capacity.hot."+os.environ["RUN_ID"]


def seed():
    start,end,next_end=bounds()
    permit=os.environ["RUN_ID"]+"/seed"
    granted=base.coord("/v1/writes/acquire",{"id":permit,"owner":os.environ["RUN_ID"]+"/seeder","days":[start[:10],end[:10]]},"LOADER")
    if sorted(granted["hot_days"])!=[start[:10],end[:10]]:raise AssertionError("seed days are not hot")
    count=0;sent=0;began=time.monotonic()
    def renew():
        base.coord("/v1/writes/renew",{"id":permit,"owner":os.environ["RUN_ID"]+"/seeder"},"LOADER")
    def checkpoint(phase,**extra):
        renew()
        base.save('seed-progress',{'phase':phase,'rows':count,'uncompressed_lp_bytes':sent,'seconds':time.monotonic()-began,**extra})
    checkpoint('writing')
    def write(payload):
        renew()
        influx_call('/api/v2/write?org=waggle&bucket=waggle&precision=ns',gzip.compress(payload,compresslevel=1),
            headers={"Authorization":"Token "+os.environ["INFLUX_TOKEN"],"Content-Type":"text/plain","Content-Encoding":"gzip"})
    try:
        for payload,n in batches(rows()):
            write(payload);count+=n;sent+=len(payload)
            if count%1_000_000<10000:checkpoint('writing')
    except BaseException:
        checkpoint('write-failed',acknowledged_rows=count)
        raise
    checkpoint('writes-complete')
    sentinel=base.ns(end)+194
    checkpoint('writing-sentinel')
    write((sentinel_name()+",node=capacity-fixture,vsn=CAPACITY,plugin=capacity value=42.5 "+str(sentinel)+"\n").encode())
    expected=base.load("source-pinned")["stats"]["rows"]
    if count!=expected:raise AssertionError('acknowledged seed/source row counts differ')
    checkpoint('counting-source')
    actual=flux_count(start,end,lambda total,windows,stop:checkpoint('counting-source',counted_rows=total,count_windows=windows,counted_through=stop))
    checkpoint('counting-sentinel',counted_rows=actual)
    retained=flux_count(end,next_end)
    if actual!=expected or retained!=1:
        raise AssertionError("seed/source row counts differ; no archive/deletion will start")
    checkpoint('counts-verified',counted_rows=actual,retained_hot_rows=retained)
    base.coord("/v1/writes/release",{"id":permit,"owner":os.environ["RUN_ID"]+"/seeder"},"LOADER")
    return {"rows":count,"lp_bytes":sent,"seconds":time.monotonic()-began,"retained_hot_sentinel":sentinel,"ingest":"direct synchronous LP under one coordinated permit; broker throughput excluded"}


def operation_reports(value, predelete=False):
    records=sorted((json.loads(path.read_text()) for path in Path(value['work']).glob('operation-*.json')),
                   key=lambda record:record['started_at'])
    required={('reconcile','EXPORTED'):['normalization','influx_extraction','exact_comparison'],
              ('verify','COMMITTED'):['object_readback','read_setup','exact_comparison','publish_hint','verification_journal']}
    if predelete:
        required.update({('verify','DELETING'):['object_readback','read_setup','exact_comparison','verification_journal'],
                         ('delete','DELETING'):['source_validation','delete_request']})
    success=set();totals={}
    for record in records:
        if record['job']!=value['id'] or record['day']!=value['day']:
            raise AssertionError('operation profile job/day mismatch')
        key=(record['operation'],record['state'])
        steps=record['steps']
        if record['passed'] and key in required:
            if [step['name'] for step in steps]!=required[key] or any(step['failed'] for step in steps):
                raise AssertionError('incomplete successful operation substeps')
            if record['threads']!=16 or not all(step['usage_available'] for step in steps):
                raise AssertionError('missing operation CPU usage or unexpected base threads')
            for step in steps:
                if step['name']=='exact_comparison':
                    detail=step.get('comparison',{})
                    if not detail.get('passed') or detail.get('mode')!='fixed' or detail.get('reused_initial') or detail.get('threads')!=16:
                        raise AssertionError('operation comparison must be fresh, fixed, exact and complete at sixteen threads')
                    expected_projection='raw_tag_dictionary' if record['operation']=='reconcile' else 'local_dictionary'
                    if detail.get('canonicalization')!=expected_projection:
                        raise AssertionError('operation comparison used wrong canonicalization policy')
            success.add(key)
        entry=totals.setdefault('/'.join(key),{'seconds_all_attempts':0,'attempts':0,'failed_attempts':0,'steps':{}})
        entry['seconds_all_attempts']+=record['total_seconds'];entry['attempts']+=1;entry['failed_attempts']+=int(not record['passed'])
        for step in steps:
            sub=entry['steps'].setdefault(step['name'],{'seconds_all_attempts':0,'failed_attempts':0,'self_cpu_seconds':0,'child_cpu_seconds':0})
            sub['seconds_all_attempts']+=step['seconds'];sub['failed_attempts']+=int(step['failed'])
            sub['self_cpu_seconds']+=step['self_cpu_seconds'];sub['child_cpu_seconds']+=step['child_cpu_seconds']
            if step.get('comparison'):
                phases=sub.setdefault('comparison_seconds_all_attempts',{})
                for phase in ('count_seconds','initial_routing_seconds','refinement_seconds','exact_seconds','binding_seconds'):
                    phases[phase]=phases.get(phase,0)+step['comparison'].get(phase,0)

    if required.keys()-success:
        raise AssertionError('missing completed operation profiles: '+str(sorted(required.keys()-success)))
    return {'attempts':records,'totals':totals}


def step_reports(work,required):
    records=[json.loads(path.read_text()) for path in Path(work).glob('step-*.json')]
    success={r['state_before'] for r in records if not r['operation_failed']}
    if required-success:return None
    if not all(r['usage_available'] for r in records):raise AssertionError('missing process resource data')
    return records


validate_normalization_policy=base.validate_normalization_policy


def validate_build_policy(profile,rows,memory):
    if (profile.get('passed') is not True or profile.get('canonicalization')!='tag_dictionary' or
        profile.get('threads')!=16 or profile.get('sort_threads')!=2 or profile.get('memory')!=memory or
        profile.get('rows')!=rows or profile.get('reuse_canonical_staging') is not True):
        raise AssertionError('actual build did not use promoted dictionary/16/2/reuse policy')
    steps=profile.get('steps',[])
    expected=[('row_count',-1)]
    for i in range((rows+9999999)//10000000):
        expected.extend((name,i) for name in ('batch_sort_copy','pack','file_sync','layout_audit','file_exact_comparison'))
    expected.append(('whole_build_exact_comparison',-1))
    if [(step.get('name'),step.get('file_index')) for step in steps]!=expected:
        raise AssertionError('missing full per-file/whole-build proof sequence')
    for step in steps:
        if step.get('failed') is not False:raise AssertionError('failed packing step')
        if step['name'] in ('file_exact_comparison','whole_build_exact_comparison'):
            proof=step.get('comparison',{})
            if (proof.get('passed') is not True or proof.get('threads')!=16 or proof.get('mode')!='fixed' or
                proof.get('reused_initial') is not (step['name']=='whole_build_exact_comparison')):
                raise AssertionError('missing fresh per-file/reused whole-build exact proof')


def routed():
    value=wait_job("ROUTED")
    base.wait('complete pre-deletion stage profiles',lambda:step_reports(value['work'],
        {'EXPORTING','EXPORTED','RECONCILED','BUILT','COMMITTED','REMOTE_VERIFIED'}),timeout=30)
    expected=base.load("source-pinned")["stats"]["rows"]
    normalization=json.loads((Path(value['work'])/'export-dedup.json').read_text())
    validate_normalization_policy(normalization)
    if normalization['unique_rows']!=expected:raise AssertionError("real engine normalization/source count mismatch")
    packing=json.loads((Path(value['work'])/'packing-profile.json').read_text())
    validate_build_policy(packing,expected,os.environ['NATIVE_MEMORY'])
    materialization=json.loads((Path(value['work'])/'expected-profile.json').read_text())
    if materialization.get('passed') is not True or materialization.get('threads')!=16 or materialization.get('memory')!=os.environ['NATIVE_MEMORY']:
        raise AssertionError('missing completed fixed-policy materialization')
    # Capture byte sizes while staging still exists; normal cleanup removes
    # these data files only after confirmed fixture deletion.
    data_bytes=sum((Path(value['work'])/Path(urlsplit(url).path).name).stat().st_size for url in value['files'])
    operations=operation_reports(value)
    return {"operations":operations,"job":value,"normalization":normalization,"build":{"materialization":materialization,"packing":packing},"data_file_bytes":data_bytes,"status":base.coord('/v1/status')}


def verify():
    value=job()
    if not value or value['state']!='ROUTED':raise AssertionError("full day has not routed")
    target=os.environ["WAREHOUSE"]
    left=source_canonical();right=canonical_scan(value['metadata'])
    comparison=canonical_equal(left,right,[target])
    report=json.loads((Path(value['work'])/'verification-COMMITTED.json').read_text())
    if not report['full_multiset_equal'] or not report['file_sha256']:raise AssertionError("missing worker full remote verification")
    return {"full_source_multiset_equal":True,"source_metadata":base.load('source-pinned')['metadata'],"destination_metadata":value['metadata'],
        "rows":base.load('source-pinned')['stats']['rows'],"comparison":comparison,"warehouse_contract":base.warehouse_contract(value['metadata']),"worker_verification":report}


def sample_query():
    # Select one observed series and a complete narrow window; never LIMIT a
    # fleet extract and then call the truncated response a complete comparison.
    first=duck("SELECT * FROM read_parquet('/reports/source.parquet') ORDER BY time LIMIT 1;")[0]
    tags=json.loads(first['tags']);filters={};predicates=[]
    # Hot and cold treat plain allowed patterns as literal equality. Anchors and
    # backslash escaping are not part of that API contract. Omit identities that
    # cannot be expressed literally; widen BOTH request and oracle together.
    for field,value in [('name',first['name'])]+[(tag,tags[tag]) for tag in ('vsn','node') if tag in tags]:
        if not isinstance(value,str) or not re.fullmatch(r'[A-Za-z0-9+_.: -]+',value):
            continue
        filters[field]=value
        column='name' if field=='name' else '(tags::JSON ->> '+base.sqlquote(field)+')'
        predicates.append(column+'='+base.sqlquote(value))
    exact=' AND '.join(predicates) or 'TRUE'
    low=first['time'];limit=int(os.environ['MAX_PROBE_ROWS'])
    for width in (1_000_000_000,1_000_000,1000,1):
        high=min(low+width,base.ns(bounds()[1]))
        result=duck("SELECT * FROM read_parquet('/reports/source.parquet') WHERE time>="+str(low)+" AND time<"+str(high)+" AND "+exact+" LIMIT "+str(limit+1)+";")
        if len(result)<=limit:
            return low,high,filters,[dict(time=r['time'],name=r['name'],value=json.loads(r['value']),meta=json.loads(r['tags'])) for r in result]
    raise AssertionError("sample series exceeds probe cap at one instant; choose a narrower identity explicitly")


def wire_equal(rows,want):
    # JSON has one numeric category: Go may emit an integral DOUBLE as `1`,
    # while DuckDB emits `1.0`. Compare exact decimal numbers without a float
    # cast/tolerance, preserving string/bool distinctions and large integers.
    def key(row):
        value=row['value']
        if isinstance(value,(int,float)) and not isinstance(value,bool):
            number=Decimal(str(value))
            value=['number','0' if number==0 else str(number.normalize())]
        else:value=[type(value).__name__,value]
        return json.dumps({**row,'value':value},sort_keys=True,separators=(',',':'))
    if Counter(map(key,rows))!=Counter(map(key,want)):
        raise AssertionError('wire record multiset differs from the pinned source sample')


def api_ready():
    # /metrics is registered after cold-backend initialization and does not
    # acquire managed read grants or execute a data query. Retry startup only;
    # the following Arrow/NDJSON/client checks still fail on their first error.
    began=time.monotonic()
    def ready():
        raw,_=base.call('http://api:10000/metrics',timeout=2)
        if b'go_goroutines' not in raw:raise AssertionError('unexpected API readiness response')
        return {'ready':True,'endpoint':'/metrics','seconds':time.monotonic()-began,'scope':'listener after cold initialization; data reads checked separately'}
    return base.wait('API startup after cold initialization',ready,timeout=240)


def api():
    import pandas as pd
    import sage_data_client
    low,high,filters,want=sample_query()
    start=pd.Timestamp(low,unit='ns',tz='UTC').isoformat();end=pd.Timestamp(high,unit='ns',tz='UTC').isoformat()
    checks={}
    for media in (base.ARROW,base.NDJSON):
        raw,headers=base.call('http://api:10000/api/v1/query',{'start':start,'end':end,'filter':filters},headers={'Content-Type':'application/json','Accept':media})
        if media==base.ARROW:
            if headers.get_content_type()!=base.ARROW:raise AssertionError('Arrow negotiation failed')
            got=base.ipc_rows(raw)
        else:
            got=[dict(time=base.ns(r['timestamp']),name=r['name'],value=r['value'],meta=r['meta']) for r in (json.loads(line) for line in raw.splitlines())]
        wire_equal(got,want);checks[media]={'rows':len(got),'bytes':len(raw)}
    import importlib
    client_module=importlib.import_module('sage_data_client.query');original_open=client_module.urlopen;media_seen=[]
    def observe(*args,**kwargs):
        response=original_open(*args,**kwargs);media=response.headers.get('Content-Type','').split(';',1)[0]
        media_seen.append(media)
        if media!=base.ARROW:response.close();raise AssertionError('default client did not receive Arrow')
        return response
    client_module.urlopen=observe
    try:frame=sage_data_client.query(start=start,end=end,filter=filters,endpoint='http://api:10000/api/v1/query')
    finally:client_module.urlopen=original_open
    got=[]
    for _,row in frame.iterrows():
        value=row['value'];value=value.item() if hasattr(value,'item') else value
        got.append(dict(time=int(pd.Timestamp(row['timestamp']).value),name=row['name'],value=value,
            meta={key[5:]:value for key,value in row.items() if key.startswith('meta.') and not pd.isna(value)}))
    wire_equal(got,want)
    _,hot_end,next_end=bounds()
    hot=base.source_rows(hot_end,next_end)
    if len(hot)!=1 or hot[0]['name']!=sentinel_name() or hot[0]['time']!=base.ns(hot_end)+194:raise AssertionError('hot boundary sentinel lost')
    return {'checks':checks,'client_rows':len(got),'client_format':media_seen[0],'filter':filters,'start':start,'end':end,'bounded_probe_only':True}


def complete():
    value=wait_job('COMPLETE');start,end,next_end=bounds()
    if flux_count(start,end)!=0 or flux_count(end,next_end)!=1:raise AssertionError('fixture deletion boundary/count mismatch')
    old=base.load('verify')['worker_verification']
    again=json.loads((Path(value['work'])/'verification-DELETING.json').read_text())
    if not again['full_multiset_equal'] or again['file_sha256']!=old['file_sha256']:raise AssertionError('missing full pre-delete re-verification')
    return {'job':value,'deletion_reverified':True,'target_day_rows':0,'retained_hot_rows':1,'status':base.coord('/v1/status')}


def summary():
    value=job();profiles=sorted((json.loads(path.read_text()) for path in Path(value['work']).glob('step-*.json')),key=lambda record:record['started_at'])
    required={'EXPORTING','EXPORTED','RECONCILED','BUILT','COMMITTED','REMOTE_VERIFIED','DELETING','DELETE_REQUESTED'}
    success={r['state_before'] for r in profiles if not r['operation_failed']}
    if required-success:raise AssertionError('missing exact stage profiles: '+str(required-success))
    if not all(r['usage_available'] for r in profiles):raise AssertionError('missing process resource data')
    totals={}
    for r in profiles:
        state=r['state_before'];entry=totals.setdefault(state,{'seconds_all_attempts':0,'attempts':0,'failed_attempts':0})
        entry['seconds_all_attempts']+=r['elapsed_seconds'];entry['attempts']+=1;entry['failed_attempts']+=int(r['operation_failed'])
    status=base.wait('all full-day grants and work drained',base.clean_status)
    verification=base.load('verify');routed_report=base.load('routed');uploaded=routed_report['data_file_bytes']
    operations=operation_reports(value,predelete=True)
    # Source upload size comes from the durable build manifest's object URLs and
    # SHA reports; local files have intentionally been cleaned after completion.
    return {'operations':operations,'build':routed_report['build'],'rows':verification['rows'],'status':status,'full_source_multiset_equal':True,'stages':totals,'attempt_profiles':profiles,
        'largest_go_peak_rss_bytes':max(r['self_peak_rss_bytes'] for r in profiles),
        'largest_child_peak_rss_bytes':max(r['largest_child_peak_rss_bytes'] for r in profiles),
        'data_file_bytes':uploaded,
        'effective_publish_bytes_per_second':uploaded/totals['BUILT']['seconds_all_attempts'],
        'effective_verify_bytes_per_second':uploaded/totals['COMMITTED']['seconds_all_attempts'],
        'throughput_scope':'data-file bytes divided by full publish/verify stage wall time; includes CPU, metadata and hash readback; not physical network bandwidth',
        'rss_scope':profiles[0]['rss_scope'],'stage_scope':'seed/extraction/API probes excluded; stage totals include operation-report IO; operations split reconciliation and verification/delete substeps; nested exact timings are included, not additional wall time'}


def process_metrics(began):
    own=resource.getrusage(resource.RUSAGE_SELF);child=resource.getrusage(resource.RUSAGE_CHILDREN)
    unit=1 if sys.platform=='darwin' else 1024
    result={'seconds':time.monotonic()-began,'parent_peak_rss_bytes':own.ru_maxrss*unit,'largest_child_peak_rss_bytes':child.ru_maxrss*unit,
        'scope':'per-probe process lifetime peaks; separate parent/child peaks are not a combined peak'}
    for name in ('memory.current','memory.peak'):
        path=Path('/sys/fs/cgroup')/name
        if path.exists():result[name]=int(path.read_text())
    return result


def main():
    stage=sys.argv[1];began=time.monotonic()
    operations={'bootstrap':bootstrap,'strings':strings,'ready':ready,'source':source,'seed':seed,'routed':routed,'verify':verify,'api-ready':api_ready,'api':api,'complete':complete,'summary':summary}
    label=stage
    if stage=='api':label='api-after-delete' if (job() or {}).get('state')=='COMPLETE' else 'api-before-delete'
    try:
        result=operations[stage]()
        base.save(label,{'passed':True,**result,'probe_resources':process_metrics(began)})
        print(stage+': passed',flush=True)
    except BaseException as error:
        message=str(error)
        for key in ('INFLUX_TOKEN','LOADER_TOKEN','READER_TOKEN','ADMIN_TOKEN'):
            if os.getenv(key):message=message.replace(os.environ[key],'<redacted>')
        base.save(label,{'passed':False,'error_type':type(error).__name__,'error':message,'probe_resources':process_metrics(began)})
        print(stage+': failed; inspect private report',file=sys.stderr);raise SystemExit(1) from None


if __name__=='__main__':main()
