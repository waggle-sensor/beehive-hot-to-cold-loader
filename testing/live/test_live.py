"""Offline checks for isolation and verification; never uses live services."""
import copy
import importlib
import importlib.util
from io import BytesIO
import json
from email.message import Message
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
import probe
import run
sys.path.insert(0,str(run.WORKSPACE / "sage-data-client/src"))


class IsolationTests(unittest.TestCase):
    def test_origin_and_local_paths_cannot_select_existing_storage(self):
        self.assertEqual(run.origin_url("https://origin:8443/sage"),"https://origin:8443/sage")
        for url in ("http://origin/sage","https://origin/node-data","https://origin/sage/cold-storage-demo/warehouse",
                    "https://origin/sage/../sage","https://name:secret@origin/sage","https://origin/sage?authz=secret"):
            with self.subTest(url=url),self.assertRaises(ValueError):run.origin_url(url)
        with self.assertRaises(ValueError):run.local("/node-data/fixture")
        with TemporaryDirectory() as directory:
            link=Path(directory)/"alias"
            link.symlink_to("/node-data",target_is_directory=True)
            with self.assertRaises(ValueError):run.local(link/"fixture")

    def test_saved_run_cannot_target_demo_project_or_warehouse(self):
        with TemporaryDirectory() as directory:
            root=Path(directory).resolve();run_id="a"*32
            config=dict(RUN_ID=run_id,LIVE_ROOT=str(root),WAREHOUSE="https://origin/sage/cold-storage-archive-live-"+run_id+"/warehouse")
            manifest=dict(project="sage-archive-live-"+run_id,warehouse=config["WAREHOUSE"],origin="https://origin/sage")
            run.save(root/"private-config.json",config);run.save(root/"run.json",manifest)
            self.assertEqual(run.Run(root).manifest["project"],manifest["project"])
            for field,value in (("project","demo"),("warehouse","https://origin/sage/cold-storage-demo/warehouse")):
                bad={**manifest,field:value};run.save(root/"run.json",bad)
                with self.subTest(field=field),self.assertRaises(ValueError):run.Run(root)
            self.assertEqual((root/"private-config.json").stat().st_mode&0o777,0o600)

    def test_non_duckdb2_fails_before_origin_io(self):
        from types import SimpleNamespace
        with patch.object(probe.subprocess,"run",return_value=SimpleNamespace(stdout="v1.5.6")),patch.object(probe,"call") as call:
            with self.assertRaises(AssertionError):probe.bootstrap()
            call.assert_not_called()


    def test_failed_readback_never_enables_deletion_and_stops_only_own_stack(self):
        with TemporaryDirectory() as directory:
            root=Path(directory).resolve();run_id="b"*32
            config=dict(RUN_ID=run_id,LIVE_ROOT=str(root),WAREHOUSE="https://origin/sage/cold-storage-archive-live-"+run_id+"/warehouse",
                        COORDINATE_ONLY="true",DELETE_ENABLED="false")
            manifest=dict(project="sage-archive-live-"+run_id,warehouse=config["WAREHOUSE"],origin="https://origin/sage",phase="prepared",passed=False)
            run.save(root/"private-config.json",config);run.save(root/"run.json",manifest)
            (root/"reports").mkdir();run.save(root/"reports/bootstrap.json",{"bucket_id":"fixture-only"})
            runner=run.Run(root)
            def stage(name):
                runner.phase(name)
                if name=="late-corrected":raise AssertionError("simulated remote readback mismatch")
            with patch.object(run.subprocess,"run"),patch("builtins.print"),patch.object(runner,"compose") as compose,patch.object(runner,"probe",side_effect=stage):
                with self.assertRaises(AssertionError):runner.execute()
                self.assertEqual(runner.config["DELETE_ENABLED"],"false")
                self.assertEqual(compose.call_args.args,("stop",))
                result=json.loads((root/"run.json").read_text())
                self.assertFalse(result["passed"])
                self.assertEqual(result["failed_phase"],"late-corrected")
                self.assertTrue(result["services_stopped"])


class VerifierTests(unittest.TestCase):
    def test_timestamp_parser_keeps_remainder_and_midnight(self):
        self.assertEqual(probe.ns("2025-03-01T00:00:00.000000194Z"),probe.START+194)
        self.assertEqual(probe.ns("2025-03-01T23:59:59.999999999+00:00"),probe.MID-1)
        self.assertEqual(probe.ns("2025-03-02T00:00:00Z"),probe.MID)
        with self.assertRaises(ValueError):probe.ns("2025-03-01T00:00:00.0000000194Z")

    def test_multiset_verifier_rejects_duplicates_type_changes_and_lost_ns(self):
        rows=probe.expected("final")
        probe.equal(list(reversed(rows)),rows)
        bads=[rows+rows[:1],[dict(row,time=(row["time"]//1000)*1000) for row in rows],
              [dict(row,value=str(row["value"])) for row in rows]]
        for bad in bads:
            with self.assertRaises(AssertionError):probe.equal(bad,rows)

    def test_empty_queue_without_ack_count_does_not_prove_acknowledgement(self):
        old=dict(messages_ready=0,messages_unacknowledged=0,message_stats={"ack":3})
        self.assertFalse(probe.queue_drained(old,4))
        self.assertTrue(probe.queue_drained({**old,"message_stats":{"ack":4}},4))
        self.assertFalse(probe.queue_drained({**old,"messages_unacknowledged":1},3))
        self.assertFalse(probe.queue_drained({},0))


    def test_initial_snapshot_check_survives_queued_late_publication_race(self):
        initial=[row for row in probe.expected("seed") if row["time"]<probe.MID]
        later=[row for row in probe.expected("fenced") if row["time"]<probe.MID]
        view={"metadata":"captured.metadata.json","frontier":probe.BOUNDS[1]}
        for captured,valid in ((later,True),([dict(row,value="unexpected") for row in later],False)):
            with patch.object(probe,"wait"),patch.object(probe,"load",return_value={}),patch.object(probe,"save") as save,patch.object(probe,"acquire_read",return_value=view),patch.object(probe,"archive_job",return_value={"metadata":"initial.metadata.json"}),patch.object(probe,"remote_rows",side_effect=lambda metadata:initial if metadata=="initial.metadata.json" else captured):
                if valid:
                    result=probe.archive_and_pin()
                    self.assertEqual(result["initial_remote_rows"],2)
                    self.assertEqual(save.call_args.args[1]["cold_rows"],later)
                else:
                    with self.assertRaises(AssertionError):probe.archive_and_pin()

    def test_source_csv_preserves_ns_and_scalar_types_and_rejects_other_fields(self):
        text="""#datatype,string,long,dateTime:RFC3339,double,string,string,string,string
#default,_result,,,,,,,
,result,table,_time,_value,_field,_measurement,node,vsn
,,0,2025-03-01T00:00:00.000000194Z,11.7,value,live.temp,n1,LIVE
#datatype,string,long,dateTime:RFC3339,string,string,string,string,string
#default,_result,,,,,,,
,result,table,_time,_value,_field,_measurement,node,vsn
,,1,2025-03-01T00:00:00.000000295Z,123,value,live.text,n1,LIVE
"""
        with patch.dict(os.environ,INFLUX_TOKEN="test-only"),patch.object(probe,"call",return_value=(text.encode(),Message())):
            rows=probe.source_rows()
        self.assertEqual(rows,[probe.record(probe.START+194,"live.temp",11.7),probe.record(probe.START+295,"live.text","123")])
        with patch.dict(os.environ,INFLUX_TOKEN="test-only"),patch.object(probe,"call",return_value=(text.replace(",value,",",other,").encode(),Message())):
            with self.assertRaises(AssertionError):probe.source_rows()

    def test_warehouse_audit_requires_v3_variant_map_ns_and_day_partition(self):
        root="https://origin/sage/cold-storage-archive-live-"+"a"*32+"/warehouse"
        body={"format-version":3,"location":root+"/sage/obs","current-schema-id":0,
              "schemas":[{"schema-id":0,"fields":[{"id":1,"name":"time","type":"timestamptz_ns"},
                {"id":11,"name":"value","type":"variant"},{"id":10,"name":"meta","type":{"type":"map"}}]}],
              "properties":{"sage.layout":"plugin-vsn-time","sage.layout.version":"bounded-plugin-v1",
                "sage.batch.order":"time","sage.row-group.rows":"262144"},
              "default-spec-id":0,"partition-specs":[{"spec-id":0,"fields":[{"source-id":1,"transform":"day"}]}]}
        bads=[]
        for mutate in (lambda b:b.update({"format-version":2}),
                       lambda b:b["schemas"][0]["fields"][0].update(type="timestamptz"),
                       lambda b:b["schemas"][0]["fields"][1].update(id=12),
                       lambda b:b["schemas"][0]["fields"][2].update(type={"type":"variant"}),
                       lambda b:b["partition-specs"][0]["fields"][0].update(transform="hour")):
            bad=copy.deepcopy(body);mutate(bad);bads.append(bad)
        with TemporaryDirectory() as directory,patch.dict(os.environ,WAREHOUSE=root),patch.object(probe,"REPORTS",Path(directory)),patch.object(probe,"token",return_value="test-token"):
            with patch.object(probe,"json_call",return_value=body):self.assertEqual(probe.warehouse_contract(root+"/sage/obs/metadata/old.metadata.json")["format"],3)
            for bad in bads:
                with patch.object(probe,"json_call",return_value=bad),self.assertRaises(AssertionError):probe.warehouse_contract(root+"/sage/obs/metadata/old.metadata.json")



@unittest.skipUnless(importlib.util.find_spec("pyarrow") and importlib.util.find_spec("pandas"),"requires local Arrow/Pandas")
class WireTests(unittest.TestCase):
    def arrow(self,rows):
        import pyarrow as pa
        import pyarrow.ipc as ipc
        table=pa.table({"timestamp":pa.array([row["time"] for row in rows],type=pa.timestamp("ns")),
                        "name":[row["name"] for row in rows],"value":[json.dumps(row["value"]) for row in rows],
                        "meta":[json.dumps(row["meta"]) for row in rows]})
        output=BytesIO()
        with ipc.new_stream(output,table.schema,options=ipc.IpcWriteOptions(compression="zstd")) as writer:writer.write_table(table)
        return output.getvalue()

    def test_all_three_decoders_compare_real_public_schema(self):
        import pandas as pd
        query_module=importlib.import_module("sage_data_client.query")
        rows=probe.expected("final")
        def select(payload):return [row for row in rows if probe.ns(payload["start"])<=row["time"]<probe.ns(payload["end"])]
        def raw_call(url,payload,headers):
            picked=select(payload);response=Message()
            if headers["Accept"]==probe.ARROW:
                response["Content-Type"]=probe.ARROW;response["X-Arrow-Compression"]="zstd"
                return self.arrow(picked),response
            # The current API does not explicitly set the NDJSON Content-Type.
            response["Content-Type"]="text/plain"
            return b"".join((json.dumps(dict(timestamp=pd.Timestamp(row["time"],unit="ns",tz="UTC").isoformat(),
                name=row["name"],value=row["value"],meta=row["meta"]))+"\n").encode() for row in picked),response
        def client_open(request):
            self.assertIn(probe.ARROW,request.headers["Accept"])
            body=BytesIO(self.arrow(select(json.loads(request.data))));body.headers=Message();body.headers["Content-Type"]=probe.ARROW
            return body
        with TemporaryDirectory() as directory,patch.object(probe,"REPORTS",Path(directory)),patch.object(probe,"call",side_effect=raw_call),patch.object(query_module,"urlopen",side_effect=client_open):
            checks=probe.check_api("offline-verifier",rows)
            self.assertEqual(len(checks),9)
            self.assertEqual(checks["mixed-client"]["rows"],5)
            self.assertEqual(checks["day-one-client"]["rows"],4)
            self.assertEqual(checks["day-two-client"]["rows"],1)

    def test_ipc_verifier_rejects_microsecond_timestamps(self):
        import pyarrow as pa
        import pyarrow.ipc as ipc
        row=probe.expected("seed")[0]
        table=ipc.open_stream(self.arrow([row])).read_all()
        table=table.set_column(0,"timestamp",table["timestamp"].cast(pa.timestamp("us"),safe=False))
        output=BytesIO()
        with ipc.new_stream(output,table.schema) as writer:writer.write_table(table)
        with self.assertRaises(AssertionError):probe.ipc_rows(output.getvalue())


if __name__=="__main__":unittest.main()
