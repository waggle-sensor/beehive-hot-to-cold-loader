#!/usr/bin/env python3
"""Full-day archival capacity gate, with read-only historical source and fresh destination."""
import argparse
from datetime import date,timedelta
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

HERE=Path(__file__).resolve().parent
LIVE=HERE.parent/"live"
spec=importlib.util.spec_from_file_location("archive_live_runner",LIVE/"run.py")
live=importlib.util.module_from_spec(spec);spec.loader.exec_module(live)
sys.path.insert(0,str(HERE))
from monitor import Monitor

SOURCE="https://nrdstor.nationalresearchplatform.org:8443/sage/cold-storage-demo-v3-bounded-full-1793018629285065000/warehouse"


def source_url(text):
    from urllib.parse import urlsplit
    import re
    u=urlsplit(text)
    if (u.scheme!="https" or not u.netloc or u.username or u.password or u.query or u.fragment
        or not re.fullmatch(r"/sage/cold-storage-[A-Za-z0-9_-]+/warehouse",u.path)):
        raise ValueError("read-only source must be an isolated cold-storage warehouse HTTPS URL")
    return text


def prepare(args):
    day=date.fromisoformat(args.day)
    if day.isoformat()!=args.day:raise ValueError("canonical UTC day required")
    source_url(args.source_warehouse)
    root=live.prepare(args)
    for name in ("full_probe.py","encoding.py"):
        shutil.copy2(HERE/name,root/"probe-build"/name)
    docker=root/"probe-build/Dockerfile"
    docker.write_text(docker.read_text().replace('ENTRYPOINT ["python", "-u", "/live/probe.py"]',
        'COPY full_probe.py encoding.py /live/\nENTRYPOINT ["python", "-u", "/live/full_probe.py"]'))
    config=json.loads((root/"private-config.json").read_text())
    config.update(SOURCE_WAREHOUSE=args.source_warehouse,SOURCE_DAY=args.day,HOT_TO_COLD_BEFORE=(day+timedelta(days=1)).isoformat(),
        NATIVE_MEMORY=args.duckdb_memory,DAY_TIMEOUT=str(args.timeout),MAX_PROBE_ROWS="5000",MIN_FREE_GIB=str(args.min_free_gib),MIN_AVAILABLE_GIB=str(args.min_available_gib),
        EXPECTED_ROWS=str(args.expected_rows if args.expected_rows is not None else (140578674 if args.source_warehouse==SOURCE and args.day=='2025-03-01' else 0)))
    if config["SOURCE_WAREHOUSE"]==config["WAREHOUSE"]:raise ValueError("source must differ from destination")
    live.save(root/"private-config.json",config)
    command=["--state=/state","--warehouse=${WAREHOUSE}","--archive-start=${SOURCE_DAY}","--archive-before=${HOT_TO_COLD_BEFORE}",
        "--influx-url=http://influx:8086","--bucket=waggle","--org=waggle","--bucket-id=${BUCKET_ID:-not-yet-discovered}",
        "--engine-path=/influx/engine","--interval=1s","--late-interval=1s","--duckdb-memory=${NATIVE_MEMORY}",
        "--duckdb-build-threads=16","--duckdb-build-sort-threads=2",
        "--writers-coordinated=true","--readers-coordinated=true","--coordinate-only=${COORDINATE_ONLY:-true}",
        "--delete-enabled=${DELETE_ENABLED:-false}","--profile-steps=true"]
    extra={key:"${"+key+"}" for key in ("SOURCE_WAREHOUSE","SOURCE_DAY","NATIVE_MEMORY","DAY_TIMEOUT","MAX_PROBE_ROWS","MIN_FREE_GIB","MIN_AVAILABLE_GIB","EXPECTED_ROWS")}
    override={"services":{
        "influx":{"mem_limit":args.influx_memory},
        "archiver":{"command":command,"mem_limit":args.archiver_memory,"volumes":["${LIVE_ROOT}/reports:/bench:ro"]},
        "probe":{"environment":extra,"mem_limit":args.probe_memory}}}
    live.save(root/"compose.capacity.json",override)
    manifest=json.loads((root/"run.json").read_text())
    manifest.update(execution_policy={"normalization_threads":16,"normalization_merge_fan_in":16,"normalization_merge_concurrency":1,"comparison_threads":16,"build_threads":16,"build_sort_threads":2,"canonical_reuse":True,"build_canonicalization":"tag_dictionary","reconciliation_canonicalization":"raw_tag_dictionary","verification_canonicalization":"local_dictionary","partitioning":"fixed"},kind="string-roundtrip" if args.strings_only else "full-day",source_warehouse=args.source_warehouse,source_day=args.day,
        limits={"influx":args.influx_memory,"archiver":args.archiver_memory,"probe":args.probe_memory,"duckdb":args.duckdb_memory},
        measurement="small string HTTP round-trip only; no capacity result" if args.strings_only else "real-day archival, excluding source extraction/seeding and bounded API probes")
    manifest["source_sha256"]["probe-build"]=live.tree_hash(root/"probe-build")
    live.save(root/"run.json",manifest)
    return root


class Run(live.Run):
    def __init__(self,root):
        super().__init__(root)
        if self.manifest.get("kind") not in ("full-day","string-roundtrip"):raise ValueError("not a capacity/string-check preparation")
        source_url(self.config["SOURCE_WAREHOUSE"])
        if self.config["SOURCE_WAREHOUSE"]==self.config["WAREHOUSE"]:raise ValueError("source/destination overlap")
        self.command.extend(["--file",str(self.root/"compose.capacity.json")])

    def probe(self,stage):
        self.phase(stage)
        # Preserve one-shot containers so OOM/exit state survives a failed probe.
        self.compose("run","--no-deps","probe",stage)

    def stop_probes(self):
        import re
        result=subprocess.run(["docker","ps","--no-trunc","--filter","label=com.docker.compose.project="+self.manifest["project"],
            "--filter","label=com.docker.compose.service=probe","--format","{{.ID}}"],capture_output=True,text=True,check=True,timeout=15)
        ids=result.stdout.split()
        if any(not re.fullmatch("[a-f0-9]{64}",value) for value in ids):raise ValueError('invalid private probe ID')
        if ids:subprocess.run(["docker","stop",*ids],check=True,timeout=90)

    def save_influx_logs(self):
        return Run.save_service_logs(self,'influx')

    def save_service_logs(self,service):
        """Retain this project's private service tail before teardown."""
        import re
        if service not in ('influx','api'):raise ValueError('unexpected diagnostic service')
        with tempfile.TemporaryFile() as output:
            result=subprocess.run(self.command+["logs","--no-color","--timestamps","--tail","200",service],
                env={**os.environ,**self.config},stdout=output,stderr=subprocess.STDOUT,timeout=20)
            output.seek(0);raw=output.read(65537)
        message=raw[:65536].decode('utf-8',errors='replace')
        for key,value in self.config.items():
            if key.endswith('_TOKEN') and value:message=message.replace(value,'<redacted>')
        message=re.sub(r'eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+','<redacted-jwt>',message)
        live.save(self.root/('reports/'+service+'-logs.json'),{'exit_code':result.returncode,'tail':message,'truncated':len(raw)>65536,
            'scope':'up to 200 log lines/64 KiB from this private project '+service+' only; captured before stop'})

    def execute(self):
        if self.manifest["phase"]!="prepared":raise ValueError("use a fresh run after an attempted execution")
        subprocess.run(["docker","info","--format","{{.OSType}}/{{.Architecture}}"],check=True)
        started=False;monitor=None
        try:
            self.phase("building")
            self.compose("config","--quiet")
            services=["probe"] if self.manifest["kind"]=="string-roundtrip" else ["archiver","api","probe"]
            self.compose("build",*services)
            started=True
            self.compose("up","-d","--wait","influx")
            monitor=Monitor(self);monitor.start()
            self.probe("bootstrap")
            self.config["BUCKET_ID"]=json.loads((self.root/"reports/bootstrap.json").read_text())["bucket_id"]
            self.probe("strings")
            if self.manifest["kind"]=="string-roundtrip":
                self.manifest["passed"]=True;self.phase("complete")
                return
            self.archiver(coordinate=True)
            self.probe("source")
            self.probe("seed")
            self.phase("archival")
            began=time.monotonic()
            self.archiver(coordinate=False)
            self.probe("routed")
            self.manifest["archive_to_routed_seconds"]=time.monotonic()-began
            self.manifest["archive_to_routed_scope"]="includes restart, startup/readiness and polling; exact worker attempts are reported separately"
            self.probe("verify")
            self.compose("up","-d","--no-deps","api")
            self.probe("api-ready")
            self.probe("api")
            # Preserve the outgoing cgroup peak before container replacement.
            monitor.sample()
            self.archiver(coordinate=False,deletion=True)
            self.probe("complete")
            self.probe("api")
            self.probe("summary")
            monitor.close();monitor=None
            resources=json.loads((self.root/"reports/resources.json").read_text())
            if not resources["archiver"]["cgroup_peak_bytes"] or not resources["influx"]["cgroup_peak_bytes"]:
                raise RuntimeError("missing kernel resource peaks; cannot claim capacity validation")
            self.manifest["passed"]=True;self.phase("complete")
        except BaseException as error:
            self.manifest.update(failed_phase=self.manifest["phase"],phase="failed",error_type=type(error).__name__)
            if started:
                for service in ('influx','api'):
                    try:self.save_service_logs(service)
                    except Exception as diagnostic_error:self.manifest[service+'_diagnostic_error_type']=type(diagnostic_error).__name__
            live.save(self.root/"run.json",self.manifest)
            raise
        finally:
            try:
                if monitor:monitor.close()
            finally:
                if started:
                    try:
                        self.compose("stop");self.stop_probes();self.manifest["services_stopped"]=True
                    except BaseException as error:
                        self.manifest.update(passed=False,phase="failed-stop",teardown_error_type=type(error).__name__);raise
                    finally:live.save(self.root/"run.json",self.manifest)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--prepare-only",action="store_true");p.add_argument("--prepared",type=Path);p.add_argument("--stop",type=Path)
    p.add_argument("--strings-only",action="store_true",help="small isolated Influx string round-trip; no historical scan or archival")
    p.add_argument("--run-dir",type=Path);p.add_argument("--day",default="2025-03-01");p.add_argument("--source-warehouse",default=SOURCE)
    p.add_argument("--origin",default="https://nrdstor.nationalresearchplatform.org:8443/sage")
    p.add_argument("--pelican-key",type=Path,default=live.WORKSPACE/"honeyhouse-config/applications/waggle-access/pelican/issuer-key.pem")
    p.add_argument("--duckdb",type=Path,default=live.ARCHIVER/"bin/duckdb")
    p.add_argument("--duckdb-memory",default="3GB");p.add_argument("--archiver-memory",default="8g")
    p.add_argument("--influx-memory",default="10g");p.add_argument("--probe-memory",default="4g")
    p.add_argument("--min-free-gib",type=int,default=32);p.add_argument("--min-available-gib",type=int,default=10)
    p.add_argument("--timeout",type=int,default=21600)
    p.add_argument("--expected-rows",type=int,help="audited row count; default March 1 source requires 140578674")
    args=p.parse_args()
    if args.expected_rows is not None and args.expected_rows<1:p.error("positive expected row count required")
    if min(args.min_free_gib,args.min_available_gib,args.timeout)<1:p.error("positive resource floors/deadline required")
    if args.prepared and (args.prepare_only or args.run_dir or args.strings_only):p.error("prepared run cannot be combined with preparation flags; its saved kind determines execution scope")
    if args.stop:
        value=Run(args.stop);value.compose("stop");value.stop_probes();return
    root=args.prepared or prepare(args)
    print("Private archival validation run:",root,flush=True)
    if not args.prepare_only:Run(root).execute()
    else:print("Prepared only; no Docker, source reads or remote writes performed.")


if __name__=="__main__":main()
