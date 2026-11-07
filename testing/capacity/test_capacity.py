"""Offline correctness/safety tests; no Docker, Influx or Pelican operations."""
import importlib.util
import csv
import gzip
import io
import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.error import URLError

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent/'live'));sys.path.insert(0,str(HERE))
spec=importlib.util.spec_from_file_location('capacity_runner',HERE/'run.py')
runner=importlib.util.module_from_spec(spec);spec.loader.exec_module(runner)
import encoding
import full_probe


def row(kind,value,**extra):
    return dict(time=1740787200000000194,name='temp',tags='{"node":"n1","sensor":"a,b c=d"}',kind=kind,value=json.dumps(value),**extra)


class EncodingTests(unittest.TestCase):
    def test_scalar_types_large_integers_escaping_and_ns(self):
        for kind,value,suffix in [('int',9007199254740993,'9007199254740993i'),('uint',18446744073709551615,'18446744073709551615u'),
                                  ('float',1.0,'1.0'),('bool',False,'false'),('string','123','"123"')]:
            line=encoding.line(row(kind,value)).decode()
            self.assertIn('sensor=a\\,b\\ c\\=d',line)
            self.assertTrue(line.endswith(' value='+suffix+' 1740787200000000194\n'))
        self.assertIn('value="a\\"b\\\\c"',encoding.line(row('string','a"b\\c')).decode())

    def test_unrepresentable_input_is_never_sanitized_or_dropped(self):
        cases=[row('string','a\rb'),row('string','a\x00b'),row('float',float('inf')),row('int',True),row('uint',-1),row('int',1<<63),row('object',{})]
        cases.append({**row('float',1.2),'tags':'{"node":""}'})
        cases.append({**row('float',1.2),'tags':'{"node":"a\\nb"}'})
        for value in cases:
            with self.subTest(value=value),self.assertRaises(ValueError):encoding.line(value)

    def test_literal_newlines_stay_inside_quoted_fields_and_batches(self):
        values=['a\nb',r'a\nb','\nleading','trailing\n','a\n\nb','quote " slash \\\nnext',
                'a\nforged,node=n1 value=99i 1740787200000000000']
        records=[row('string',value) for value in values]
        for value in values:
            expected='"'+value.replace('\\','\\\\').replace('"','\\"')+'"'
            self.assertEqual(encoding.scalar('string',json.dumps(value)),expected)
        self.assertNotEqual(encoding.line(records[0]),encoding.line(records[1]))
        groups=list(encoding.batches(records,row_limit=2,byte_limit=350))
        self.assertEqual(sum(n for _,n in groups),len(records))
        self.assertEqual(b''.join(p for p,_ in groups),b''.join(encoding.line(r) for r in records))
        self.assertTrue(all(n<=2 and len(p)<=350 for p,n in groups))

    def test_batches_are_bounded_and_preserve_all_rows(self):
        records=[row('int',i) for i in range(7)]
        groups=list(encoding.batches(records,row_limit=2,byte_limit=200))
        self.assertEqual(sum(n for _,n in groups),7)
        self.assertEqual(b''.join(payload for payload,_ in groups),b''.join(encoding.line(r) for r in records))
        self.assertTrue(all(n<=2 and len(payload)<=200 for payload,n in groups))
        with self.assertRaises(ValueError):list(encoding.batches([row('string','x'*1000)],byte_limit=100))


class GuardTests(unittest.TestCase):
    def test_api_readiness_waits_for_startup_without_querying_data(self):
        with patch.object(full_probe.base,'call',side_effect=[URLError('connection refused'),(b'# HELP go_goroutines\ngo_goroutines 8\n',{})]) as call,patch.object(full_probe.base.time,'sleep'):
            result=full_probe.api_ready()
        self.assertTrue(result['ready'])
        self.assertEqual(call.call_count,2)
        for c in call.call_args_list:
            self.assertEqual(c.args,('http://api:10000/metrics',));self.assertEqual(c.kwargs['timeout'],2)
        with patch.object(full_probe.base,'call',return_value=(b'unexpected service',{})):
            with self.assertRaises(AssertionError):full_probe.api_ready()

    def test_api_startup_failure_does_not_enable_deletion(self):
        with TemporaryDirectory() as directory:
            root=Path(directory).resolve();run_id='e'*32
            config=dict(RUN_ID=run_id,LIVE_ROOT=str(root),WAREHOUSE='https://origin/sage/cold-storage-archive-live-'+run_id+'/warehouse',
                SOURCE_WAREHOUSE=runner.SOURCE,COORDINATE_ONLY='true',DELETE_ENABLED='false')
            runner.live.save(root/'private-config.json',config)
            runner.live.save(root/'run.json',dict(project='sage-archive-live-'+run_id,warehouse=config['WAREHOUSE'],origin='https://origin/sage',phase='prepared',kind='full-day',passed=False))
            (root/'reports').mkdir();runner.live.save(root/'reports/bootstrap.json',{'bucket_id':'isolated'})
            run=runner.Run(root);stages=[]
            def probe(stage):
                stages.append(stage);run.phase(stage)
                if stage=='api-ready':raise TimeoutError('API did not listen')
            with patch.object(runner.subprocess,'run'),patch.object(runner,'Monitor'),patch.object(run,'compose'),patch.object(run,'probe',side_effect=probe),patch.object(run,'archiver') as archiver,patch.object(run,'save_service_logs') as logs,patch('builtins.print'):
                with self.assertRaises(TimeoutError):run.execute()
            self.assertNotIn('api',stages);self.assertNotIn('complete',stages)
            self.assertFalse(any(c.kwargs.get('deletion') for c in archiver.call_args_list))
            self.assertEqual(run.manifest['failed_phase'],'api-ready');self.assertEqual(run.config['DELETE_ENABLED'],'false')
            self.assertEqual([c.args[0] for c in logs.call_args_list],['influx','api'])

    def test_count_windows_cover_exact_range_without_gaps_or_overlap(self):
        start='2025-03-01T00:00:00.000000194Z';end='2025-03-01T00:10:00.000000295Z'
        updates=[]
        with patch.object(full_probe,'flux_count_window',side_effect=[3,5,7]) as query:
            self.assertEqual(full_probe.flux_count(start,end,lambda *v:updates.append(v)),15)
        windows=[c.args for c in query.call_args_list]
        self.assertEqual(windows[0][0],start);self.assertEqual(windows[-1][1],end)
        for left,right in zip(windows,windows[1:]):self.assertEqual(left[1],right[0])
        self.assertTrue(all(0<full_probe.base.ns(b)-full_probe.base.ns(a)<=300_000_000_000 for a,b in windows))
        self.assertEqual(updates[-1],(15,3,end))

    def test_count_rejects_error_tables_and_malformed_csv(self):
        cases=[b',error,reference\n,query failed,1\n',b',,0,12\n',b',result,table,_value\n,,0,-1\n',
            b',result,table,_value\n,,0,1\n,,1,2\n']
        with patch.dict(os.environ,INFLUX_TOKEN='secret'):
            for body in cases:
                with self.subTest(body=body),patch.object(full_probe,'influx_call',return_value=(body,{})):
                    with self.assertRaises(AssertionError):full_probe.flux_count_window('start','end')

    def test_http_500_body_is_bounded_redacted_and_original_error_preserved(self):
        body=b'cache maximum memory size exceeded private-secret eyJabc.def.signature '+b'x'*9000
        error=HTTPError('http://influx:8086/api/v2/write',500,'server error',{},io.BytesIO(body))
        with patch.dict(os.environ,INFLUX_TOKEN='private-secret'),patch.object(full_probe.base,'urlopen',side_effect=error),patch.object(full_probe.base,'save') as save:
            with self.assertRaisesRegex(RuntimeError,'HTTP 500'):
                full_probe.influx_call('/api/v2/write?org=waggle&bucket=waggle&precision=ns',b'private point',headers={})
            label,report=save.call_args.args
            self.assertEqual(label,'influx-http-error');self.assertEqual(report['status'],500)
            self.assertIn('cache maximum memory size exceeded',report['body'])
            self.assertNotIn('private-secret',report['body']);self.assertNotIn('eyJabc',report['body'])
            self.assertTrue(report['body_truncated']);self.assertNotIn('private point',report['body'])
        error=HTTPError('http://influx:8086/api/v2/query',500,'server error',{},io.BytesIO(b'failed'))
        with patch.object(full_probe.base,'urlopen',side_effect=error),patch.object(full_probe.base,'save',side_effect=OSError('disk full')):
            with self.assertRaisesRegex(RuntimeError,'HTTP 500'):
                full_probe.influx_call('/api/v2/query?org=waggle',{},headers={})

    def test_seed_reports_writes_complete_before_count_failure(self):
        with patch.dict(os.environ,SOURCE_DAY='2025-03-01',RUN_ID='test',INFLUX_TOKEN='private'),patch.object(full_probe.base,'coord',return_value={'hot_days':['2025-03-01','2025-03-02']}) as coord,patch.object(full_probe.base,'save') as save,patch.object(full_probe.base,'load',return_value={'stats':{'rows':2}}),patch.object(full_probe,'rows'),patch.object(full_probe,'batches',return_value=[(b'one',1),(b'two',1)]),patch.object(full_probe,'influx_call'),patch.object(full_probe,'flux_count',side_effect=RuntimeError('connection closed')):
            with self.assertRaisesRegex(RuntimeError,'connection closed'):full_probe.seed()
        progress=[c.args[1] for c in save.call_args_list if c.args[0]=='seed-progress']
        self.assertIn('writes-complete',[p['phase'] for p in progress])
        self.assertEqual(progress[-1]['phase'],'counting-source');self.assertEqual(progress[-1]['rows'],2)
        paths=[call.args[0] for call in coord.call_args_list]
        self.assertEqual(paths.count('/v1/writes/acquire'),1)
        self.assertIn('/v1/writes/renew',paths)
        self.assertNotIn('/v1/writes/release',paths) # Failed seed stops renewal; expiry will drain the permit.

    def test_failure_logs_are_private_project_only_bounded_and_redacted(self):
        with TemporaryDirectory() as directory:
            root=Path(directory);(root/'reports').mkdir()
            run=SimpleNamespace(root=root,config={'INFLUX_TOKEN':'private-secret'},command=['docker','compose','--project-name','private-project'])
            def capture(command,**kwargs):
                self.assertEqual(command,run.command+['logs','--no-color','--timestamps','--tail','200','influx'])
                kwargs['stdout'].write(b'private-secret eyJabc.def.signature cache error\n'+b'x'*70000)
                return SimpleNamespace(returncode=0)
            with patch.object(runner.subprocess,'run',side_effect=capture):runner.Run.save_influx_logs(run)
            report=json.loads((root/'reports/influx-logs.json').read_text())
            self.assertIn('cache error',report['tail']);self.assertTrue(report['truncated'])
            self.assertNotIn('private-secret',report['tail']);self.assertNotIn('eyJabc',report['tail'])

    def test_live_string_gate_checks_gzipped_http_readback_before_source_scan(self):
        values=[]
        corrupt=False
        def call(url,data=None,**kwargs):
            if '/write?' in url:
                payload=gzip.decompress(data)
                self.assertIn(b'first\nsecond',payload)
                self.assertIn(b'literal\\\\ntext',payload)
                return b'',{}
            self.assertIn('/query?',url)
            output=io.StringIO(newline='');writer=csv.writer(output,lineterminator='\n')
            writer.writerow(['','result','table','_time','_value','_field','_measurement','node'])
            for i,value in enumerate(values):
                writer.writerow(['','','0',f'2025-03-01T00:00:00.{194+i:09d}Z',value,'value','capacity.strings','capacity-fixture'])
            raw=output.getvalue().encode()
            if corrupt:raw=raw.replace(b'first\nsecond',b'first\\nsecond')
            # Match the live Influx 2.7.7 annotated CSV dialect.
            return raw.replace(b'\n',b'\r\n'),{}
        original=full_probe.batches
        def capture(records):
            values.extend(json.loads(r['value']) for r in records)
            return original(records)
        bucket={'id':'private','name':'capacity-strings-test','orgID':'org'}
        with patch.dict(os.environ,SOURCE_DAY='2025-03-01',RUN_ID='test',INFLUX_TOKEN='private'),patch.object(full_probe.base,'json_call',side_effect=[{'buckets':[{'name':'waggle','orgID':'org'}]},bucket]),patch.object(full_probe.base,'call',side_effect=call),patch.object(full_probe,'batches',side_effect=capture):
            result=full_probe.strings()
        self.assertEqual(result['rows'],7)
        self.assertTrue(result['literal_lf_preserved'])
        corrupt=True;values.clear()
        with patch.dict(os.environ,SOURCE_DAY='2025-03-01',RUN_ID='test',INFLUX_TOKEN='private'),patch.object(full_probe.base,'json_call',side_effect=[{'buckets':[{'name':'waggle','orgID':'org'}]},bucket]),patch.object(full_probe.base,'call',side_effect=call),patch.object(full_probe,'batches',side_effect=capture):
            with self.assertRaises(AssertionError):full_probe.strings()

    def test_strings_only_never_starts_archival_or_source_scan(self):
        with TemporaryDirectory() as directory:
            root=Path(directory).resolve();run_id='d'*32
            config=dict(RUN_ID=run_id,LIVE_ROOT=str(root),WAREHOUSE='https://origin/sage/cold-storage-archive-live-'+run_id+'/warehouse',
                SOURCE_WAREHOUSE=runner.SOURCE,COORDINATE_ONLY='true',DELETE_ENABLED='false')
            runner.live.save(root/'private-config.json',config)
            runner.live.save(root/'run.json',dict(project='sage-archive-live-'+run_id,warehouse=config['WAREHOUSE'],origin='https://origin/sage',phase='prepared',kind='string-roundtrip',passed=False))
            (root/'reports').mkdir();runner.live.save(root/'reports/bootstrap.json',{'bucket_id':'isolated'})
            run=runner.Run(root)
            with patch.object(runner.subprocess,'run'),patch.object(runner,'Monitor'),patch.object(run,'compose'),patch.object(run,'probe') as probe,patch.object(run,'archiver') as archiver:
                run.execute()
            self.assertEqual([c.args[0] for c in probe.call_args_list],['bootstrap','strings'])
            archiver.assert_not_called()
            self.assertTrue(run.manifest['passed']);self.assertTrue(run.manifest['services_stopped'])
            self.assertEqual(run.config['DELETE_ENABLED'],'false')

    def test_source_url_is_read_only_warehouse_not_arbitrary_path(self):
        self.assertEqual(runner.source_url(runner.SOURCE),runner.SOURCE)
        for url in ('http://origin/sage/cold-storage-demo/warehouse','https://origin/node-data','https://u:p@origin/sage/cold-storage-demo/warehouse',
                    'https://origin/sage/cold-storage-demo/warehouse?authz=secret','https://origin/sage/cold-storage-demo/../warehouse'):
            with self.assertRaises(ValueError):runner.source_url(url)

    def test_source_multiset_mismatch_stops_before_delete(self):
        with TemporaryDirectory() as directory:
            root=Path(directory).resolve();run_id='c'*32
            config=dict(RUN_ID=run_id,LIVE_ROOT=str(root),WAREHOUSE='https://origin/sage/cold-storage-archive-live-'+run_id+'/warehouse',
                SOURCE_WAREHOUSE=runner.SOURCE,COORDINATE_ONLY='true',DELETE_ENABLED='false')
            manifest=dict(project='sage-archive-live-'+run_id,warehouse=config['WAREHOUSE'],origin='https://origin/sage',phase='prepared',kind='full-day',passed=False)
            runner.live.save(root/'private-config.json',config);runner.live.save(root/'run.json',manifest)
            (root/'reports').mkdir();runner.live.save(root/'reports/bootstrap.json',{'bucket_id':'isolated'})
            run=runner.Run(root)
            def probe(stage):
                run.phase(stage)
                if stage=='verify':raise AssertionError('full source comparison differed')
            with patch.object(runner.subprocess,'run'),patch.object(runner,'Monitor'),patch.object(run,'compose') as compose,patch.object(run,'probe',side_effect=probe),patch('builtins.print'):
                with self.assertRaises(AssertionError):run.execute()
                self.assertEqual(run.config['DELETE_ENABLED'],'false')
                self.assertEqual(compose.call_args.args,('stop',))
                result=json.loads((root/'run.json').read_text())
                self.assertFalse(result['passed']);self.assertEqual(result['failed_phase'],'verify')

    def test_full_comparison_rejects_differences(self):
        with patch.dict(os.environ,WAREHOUSE='https://origin/sage/cold-storage-archive-test/warehouse',SOURCE_DAY='2025-03-01'),patch.object(full_probe,'job',return_value={'state':'ROUTED','metadata':'https://origin/metadata.json'}),patch.object(full_probe,'duck',return_value=[{'a':2,'b':1}]) as duck:
            with self.assertRaises(AssertionError):full_probe.verify()
            sql=duck.call_args.args[0]
            self.assertIn('count(*)',sql)
            self.assertIn('native_kind',sql)

    def test_partitioned_reference_stages_once_checks_completion_and_cleans_up(self):
        import re
        import pyarrow as pa
        import pyarrow.parquet as pq
        with TemporaryDirectory() as directory:
            root=Path(directory)
            calls=[]
            def native(sql,warehouses=()):
                calls.append((sql,warehouses))
                if ' AS a,' in sql:return [{'a':2,'b':2}]
                if 'COPY (' in sql:
                    for target in re.findall(" TO '([^']+)'",sql):
                        path=Path(target)/'shard=0';path.mkdir(parents=True)
                        pq.write_table(pa.table({'time':[1740787200000000194]*2,'name':['x']*2,'tags':['{}']*2,'kind':['string']*2,'value':['"a\\nb"']*2}),path/'data.parquet')
                    return []
                self.assertEqual(sql.count('EXCEPT ALL'),2)
                self.assertIn('SELECT time,name,tags,kind,value FROM read_parquet',sql)
                self.assertFalse(warehouses)
                return [{'all_partitions_equal':True}]
            with patch.object(full_probe,'REPORTS',root),patch.object(full_probe,'duck',side_effect=native):
                result=full_probe.canonical_equal('left','right',['scoped-read'])
            self.assertEqual(result['rows'],2);self.assertTrue(result['exact_multiset_equal'])
            self.assertEqual(len(calls),3);self.assertEqual(list(root.iterdir()),[])

    def test_native_disk_guard_refuses_before_launch(self):
        with patch.object(full_probe.shutil,'disk_usage',return_value=SimpleNamespace(free=8*(1<<30))),patch.object(full_probe.subprocess,'run') as native:
            with self.assertRaisesRegex(RuntimeError,'8 GiB'):full_probe.duck('SELECT 1')
            native.assert_not_called()

    def test_api_probe_reduces_window_instead_of_truncating_rows(self):
        first=row('float',1.2)
        result=[row('float',1.2)]
        with patch.dict(os.environ,SOURCE_DAY='2025-03-01',MAX_PROBE_ROWS='2'),patch.object(full_probe,'duck',side_effect=[[first],[first]*3,result]) as duck:
            low,high,filters,records=full_probe.sample_query()
            self.assertEqual(high-low,1000000)
            self.assertEqual(len(records),1)
            self.assertEqual(filters['name'],'temp')
            self.assertIn("name='temp'",duck.call_args_list[1].args[0])
            self.assertIn('LIMIT 3',duck.call_args_list[1].args[0])

    def test_api_probe_uses_literal_filters_without_regex_escaping(self):
        first={**row('float',1.2),'name':'uncorrected_inner.+-: value','tags':json.dumps({'vsn':'W099','node':'000048b02dd3c51f'})}
        with patch.dict(os.environ,SOURCE_DAY='2025-03-01',MAX_PROBE_ROWS='2'),patch.object(full_probe,'duck',side_effect=[[first],[first]]) as duck:
            _,_,filters,_=full_probe.sample_query()
        self.assertEqual(filters,{'name':first['name'],'vsn':'W099','node':'000048b02dd3c51f'})
        self.assertIn("name='uncorrected_inner.+-: value'",duck.call_args_list[1].args[0])
        self.assertNotIn('^',str(filters));self.assertNotIn('\\',str(filters))

    def test_api_probe_unsupported_literals_widen_request_and_oracle_together(self):
        for name,node in [('a|b','a*b'),('a/b','a\nb'),('温度','quoted"node')]:
            first={**row('float',1.2),'name':name,'tags':json.dumps({'node':node,'vsn':'W099'})}
            with patch.dict(os.environ,SOURCE_DAY='2025-03-01',MAX_PROBE_ROWS='2'),patch.object(full_probe,'duck',side_effect=[[first],[first]]) as duck:
                _,_,filters,records=full_probe.sample_query()
            self.assertEqual(filters,{'vsn':'W099'});self.assertEqual(len(records),1)
            sql=duck.call_args_list[1].args[0]
            self.assertNotIn(' AND name=',sql);self.assertNotIn("->> 'node'",sql)
            self.assertIn("(tags::JSON ->> 'vsn')='W099'",sql)

    def test_api_probe_with_no_literal_filters_compares_complete_time_window(self):
        first={**row('float',1.2),'name':'a|b','tags':json.dumps({'node':'a*b'})}
        with patch.dict(os.environ,SOURCE_DAY='2025-03-01',MAX_PROBE_ROWS='2'),patch.object(full_probe,'duck',side_effect=[[first],[first]]) as duck:
            _,_,filters,records=full_probe.sample_query()
        self.assertEqual(filters,{})
        self.assertEqual(len(records),1)
        self.assertIn(' AND TRUE LIMIT 3',duck.call_args_list[1].args[0])

    def test_json_numeric_spelling_is_equal_without_hiding_type_or_precision_loss(self):
        expected=[dict(time=1740787200000000194,name='t',value=1.0,meta={})]
        full_probe.wire_equal([{**expected[0],'value':1}],expected)
        for value in ('1',True):
            with self.assertRaises(AssertionError):full_probe.wire_equal([{**expected[0],'value':value}],expected)
        exact=[{**expected[0],'value':9007199254740993}]
        with self.assertRaises(AssertionError):full_probe.wire_equal([{**exact[0],'value':9007199254740992.0}],exact)

    def test_native_errors_preserve_resource_diagnostics_but_not_credentials(self):
        with patch.dict(os.environ,NATIVE_MEMORY='3GB',DAY_TIMEOUT='5'),patch.object(full_probe.shutil,'disk_usage',return_value=SimpleNamespace(free=64*(1<<30))),patch.object(full_probe.base,'save') as save:
            for stderr in ('Out of Memory: failed allocation','Bearer eyJabc.def.signature'):
                with patch.object(full_probe.subprocess,'run',return_value=SimpleNamespace(returncode=1,stderr=stderr)):
                    with self.assertRaises(RuntimeError):full_probe.duck('SELECT 1')
                stored=save.call_args.args[1]['stderr']
                if stderr.startswith('Out'):self.assertIn('Out of Memory',stored)
                else:self.assertNotIn('eyJabc',stored)



class ExecutionPolicyTests(unittest.TestCase):
    def test_capacity_and_live_commands_keep_build_settings_scoped(self):
        with TemporaryDirectory() as temporary:
            root=Path(temporary);(root/'probe-build').mkdir()
            (root/'probe-build/Dockerfile').write_text('ENTRYPOINT ["python", "-u", "/live/probe.py"]')
            runner.live.save(root/'private-config.json',{'WAREHOUSE':'https://origin/sage/cold-storage-test/warehouse'})
            runner.live.save(root/'run.json',{'source_sha256':{}})
            args=SimpleNamespace(day='2025-03-01',source_warehouse=runner.SOURCE,duckdb_memory='3GB',timeout=21600,
                min_free_gib=32,min_available_gib=10,expected_rows=None,influx_memory='10g',archiver_memory='8g',probe_memory='4g',strings_only=False)
            with patch.object(runner.live,'prepare',return_value=root):runner.prepare(args)
            command=json.loads((root/'compose.capacity.json').read_text())['services']['archiver']['command']
            live_compose=(HERE.parent/'live/compose.yml').read_text()
            for flag in ('--duckdb-build-threads=16','--duckdb-build-sort-threads=2'):
                self.assertIn(flag,command);self.assertIn('- '+flag,live_compose)
            self.assertFalse(any(flag.startswith('--duckdb-comparison-threads') for flag in command))
            self.assertNotIn('--duckdb-comparison-threads',live_compose)
            self.assertNotIn('--duckdb-threads',live_compose)
            self.assertFalse(any(flag.startswith('--duckdb-threads') for flag in command))
            self.assertEqual(json.loads((root/'run.json').read_text())['execution_policy'],
                {'normalization_threads':16,'normalization_merge_fan_in':16,'normalization_merge_concurrency':1,'comparison_threads':16,'build_threads':16,'build_sort_threads':2,'canonical_reuse':True,'build_canonicalization':'tag_dictionary','reconciliation_canonicalization':'raw_tag_dictionary','verification_canonicalization':'local_dictionary','partitioning':'fixed'})


class BuildPolicyTests(unittest.TestCase):
    def fixture(self):
        steps=[dict(name='row_count',file_index=-1,failed=False)]
        for index in range(15):
            for name in ('batch_sort_copy','pack','file_sync','layout_audit','file_exact_comparison'):
                entry=dict(name=name,file_index=index,failed=False)
                if name=='file_exact_comparison':entry['comparison']=dict(passed=True,threads=16,mode='fixed',reused_initial=False)
                steps.append(entry)
        steps.append(dict(name='whole_build_exact_comparison',file_index=-1,failed=False,
            comparison=dict(passed=True,threads=16,mode='fixed',reused_initial=True)))
        return dict(passed=True,canonicalization='tag_dictionary',threads=16,sort_threads=2,memory='3GB',rows=140578674,reuse_canonical_staging=True,steps=steps)

    def test_actual_promoted_policy_and_every_proof_required(self):
        import copy
        profile=self.fixture();full_probe.validate_build_policy(profile,140578674,'3GB')
        for label in ('legacy','sort','rows','memory','file','whole','sequence','failure'):
            value=copy.deepcopy(profile)
            if label=='legacy':value.pop('canonicalization')
            elif label=='sort':value['sort_threads']=16
            elif label=='rows':value['rows']-=1
            elif label=='memory':value['memory']='6GB'
            elif label=='file':value['steps'][5]['comparison']['reused_initial']=True
            elif label=='whole':value['steps'][-1]['comparison']['passed']=False
            elif label=='sequence':value['steps'].pop(10)
            elif label=='failure':value['steps'][4]['failed']=True
            with self.subTest(label=label),self.assertRaises(AssertionError):
                full_probe.validate_build_policy(value,140578674,'3GB')


class OperationProfileTests(unittest.TestCase):
    def fixture(self,root):
        value={'id':'job','day':'2025-03-01','work':str(root)}
        specs=[('reconcile','EXPORTED',['normalization','influx_extraction','exact_comparison']),
               ('verify','COMMITTED',['object_readback','read_setup','exact_comparison','publish_hint','verification_journal']),
               ('verify','DELETING',['object_readback','read_setup','exact_comparison','verification_journal']),
               ('delete','DELETING',['source_validation','delete_request'])]
        records=[]
        for i,(operation,state,names) in enumerate(specs):
            steps=[dict(name=name,seconds=1,failed=False,usage_available=True,self_cpu_seconds=.1,child_cpu_seconds=.2) for name in names]
            for step in steps:
                if step['name']=='exact_comparison':step['comparison']={'passed':True,'mode':'fixed','reused_initial':False,'threads':16,'canonicalization':'raw_tag_dictionary' if operation=='reconcile' else 'local_dictionary'}
            record=dict(job='job',day=value['day'],operation=operation,state=state,started_at=str(i),threads=16,passed=True,total_seconds=len(steps),steps=steps)
            (root/('operation-'+str(i)+'.json')).write_text(json.dumps(record));records.append(record)
        return value,records

    def test_operation_reports_split_reconciliation_and_delete(self):
        with TemporaryDirectory() as temp:
            root=Path(temp);value,records=self.fixture(root)
            result=full_probe.operation_reports(value,predelete=True)
            self.assertEqual(len(result['attempts']),4)
            self.assertEqual(result['totals']['reconcile/EXPORTED']['steps']['influx_extraction']['seconds_all_attempts'],1)
            self.assertEqual(result['totals']['verify/DELETING']['seconds_all_attempts'],4)
            self.assertEqual(result['totals']['delete/DELETING']['steps']['delete_request']['seconds_all_attempts'],1)
            failed={**records[0],'passed':False,'steps':[dict(records[0]['steps'][0],failed=True)]}
            (root/'operation-failed.json').write_text(json.dumps(failed))
            result=full_probe.operation_reports(value,predelete=True)
            self.assertEqual(result['totals']['reconcile/EXPORTED']['attempts'],2)
            self.assertEqual(result['totals']['reconcile/EXPORTED']['failed_attempts'],1)

    def test_missing_profile_or_incomplete_steps_cannot_pass(self):
        with TemporaryDirectory() as temp:
            root=Path(temp);value,records=self.fixture(root)
            (root/'operation-3.json').unlink()
            full_probe.operation_reports(value) # Routing precedes deletion.
            with self.assertRaisesRegex(AssertionError,'missing completed'):full_probe.operation_reports(value,True)
            records[0]['steps'].pop();(root/'operation-0.json').write_text(json.dumps(records[0]))
            with self.assertRaisesRegex(AssertionError,'incomplete'):full_probe.operation_reports(value)

    def test_reused_remote_proof_and_mismatched_job_are_rejected(self):
        with TemporaryDirectory() as temp:
            root=Path(temp);value,records=self.fixture(root)
            records[1]['steps'][2]['comparison']['reused_initial']=True
            (root/'operation-1.json').write_text(json.dumps(records[1]))
            with self.assertRaisesRegex(AssertionError,'fresh'):full_probe.operation_reports(value)
            records[1]['steps'][2]['comparison']['reused_initial']=False
            records[1]['steps'][2]['comparison']['threads']=2
            (root/'operation-1.json').write_text(json.dumps(records[1]))
            with self.assertRaisesRegex(AssertionError,'sixteen'):full_probe.operation_reports(value)
            records[1]['job']='other';(root/'operation-1.json').write_text(json.dumps(records[1]))
            with self.assertRaisesRegex(AssertionError,'job/day'):full_probe.operation_reports(value)

    def test_reconciliation_and_remote_projection_policies_are_distinct(self):
        for index,projection in ((0,'baseline'),(1,'raw_tag_dictionary'),(2,None),(1,'baseline'),(1,'both_dictionary')):
            with TemporaryDirectory() as temp:
                root=Path(temp);value,records=self.fixture(root)
                detail=next(step['comparison'] for step in records[index]['steps'] if step['name']=='exact_comparison')
                if projection is None:detail.pop('canonicalization')
                else:detail['canonicalization']=projection
                (root/('operation-'+str(index)+'.json')).write_text(json.dumps(records[index]))
                with self.assertRaisesRegex(AssertionError,'canonicalization'):full_probe.operation_reports(value,predelete=True)

class NormalizationPolicyTests(unittest.TestCase):
    def test_normalization_policy_requires_measured_settings_and_passes(self):
        good=dict(threads=16,merge_fan_in=16,merge_concurrency=1,partitions=256,max_merge_fan_in=16,merge_passes=2)
        full_probe.validate_normalization_policy(good)
        full_probe.validate_normalization_policy(dict(good,partitions=1,max_merge_fan_in=1,merge_passes=1))
        for key,value in [('threads',2),('merge_fan_in',8),('merge_concurrency',2),('max_merge_fan_in',8),('merge_passes',3),('partitions',0)]:
            with self.assertRaises(AssertionError):full_probe.validate_normalization_policy(dict(good,**{key:value}))

    def test_old_base_thread_evidence_is_rejected(self):
        with TemporaryDirectory() as temp:
            root=Path(temp);value,records=OperationProfileTests().fixture(root)
            records[0]['threads']=2
            (root/'operation-0.json').write_text(json.dumps(records[0]))
            with self.assertRaisesRegex(AssertionError,'base threads'):full_probe.operation_reports(value)

if __name__=='__main__':unittest.main()
