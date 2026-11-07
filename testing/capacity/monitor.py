"""Sample only recorded project containers. Kernel cgroup peaks include cache."""
import json
import re
import subprocess
import threading
import time

# Fixed paths refer only to mounts of the private benchmark project.
SCRIPT = '''
for field in memory.current memory.peak; do
 if [ -r /sys/fs/cgroup/$field ]; then
  printf '%s ' "$field"
  cat /sys/fs/cgroup/$field
 fi
done
'''
DISK = '''
for directory in /state /state/spill /influx /bench /bench/native-spill; do
 if [ -d "$directory" ]; then
  printf 'logical '
  du -sb "$directory"
  printf 'allocated '
  du -s -B1 "$directory"
 fi
done
'''


class Monitor:
    def __init__(self, runner, interval=5):
        self.runner=runner;self.interval=interval
        self.stop_event=threading.Event();self.lock=threading.Lock()
        self.samples=[];self.errors=[];self.seen_stopped=set()
        self.thread=threading.Thread(target=self.loop,daemon=True)

    def start(self):self.thread.start()

    def sample(self):
        with self.lock:
            for role in ("influx","archiver","probe","api"):
                try:
                    listed=subprocess.run(["docker","ps","--all","--no-trunc","--filter","label=com.docker.compose.project="+self.runner.manifest["project"],
                        "--filter","label=com.docker.compose.service="+role,"--format","{{.ID}}"],capture_output=True,text=True,check=True,timeout=15)
                    ids=listed.stdout.split()
                    for container in ids:
                        if not re.fullmatch("[a-f0-9]{12,64}",container):raise ValueError("invalid private container ID")
                        if container in self.seen_stopped:continue
                        inspected=subprocess.run(["docker","inspect","--format","{{json .State}}",container],capture_output=True,text=True,check=True,timeout=15)
                        state=json.loads(inspected.stdout)
                        sample={"utc":time.time(),"phase":self.runner.manifest["phase"],"role":role,"container":container,
                                "state":{"status":state['Status'],"oom_killed":state['OOMKilled'],"exit_code":state['ExitCode']}}
                        if not state['Running']:
                            self.seen_stopped.add(container)
                            self.record(sample)
                            continue
                        result=subprocess.run(["docker","exec",container,"sh","-c",SCRIPT+(DISK if role=="archiver" else "")],capture_output=True,text=True,timeout=15)
                        if result.returncode:
                            sample['collection_exit_code']=result.returncode
                        for line in result.stdout.splitlines():
                            fields=line.split()
                            if len(fields)>=2 and fields[0].startswith("memory.") and fields[1].isdigit():
                                sample[fields[0]]=int(fields[1])
                            elif len(fields)>=3 and fields[0] in ("logical","allocated") and fields[1].isdigit():
                                sample.setdefault("disk",{}).setdefault(fields[2],{})[fields[0]]=int(fields[1])
                        self.record(sample)
                except (OSError,subprocess.SubprocessError,ValueError) as error:
                    self.errors.append({"role":role,"error_type":type(error).__name__,"utc":time.time()})

    def record(self,sample):
        self.samples.append(sample)
        with (self.runner.root/"reports/resources.jsonl").open("a") as out:out.write(json.dumps(sample)+"\n")

    def loop(self):
        while not self.stop_event.is_set():
            self.sample();self.stop_event.wait(self.interval)

    def close(self):
        self.stop_event.set();self.thread.join(timeout=90)
        if self.thread.is_alive():raise RuntimeError("resource sampler did not stop")
        self.sample()
        summary={"interval_seconds":self.interval,"samples":len(self.samples),"errors":self.errors,
                 "oom_events":[s for s in self.samples if s.get("state",{}).get("oom_killed")],
                 "scope":"per-container cgroup lifetime peaks include page cache; disk samples can miss transient/unlinked files; role peaks are not additive"}
        for role in ("influx","archiver","probe","api"):
            records=[s for s in self.samples if s["role"]==role]
            peaks=[s["memory.peak"] for s in records if "memory.peak" in s]
            summary[role]={"cgroup_peak_bytes":max(peaks) if peaks else None,"containers":sorted({s["container"] for s in records})}
        disks={}
        for record in self.samples:
            for path,values in record.get("disk",{}).items():
                for kind,value in values.items():disks.setdefault(path,{})[kind]=max(disks.get(path,{}).get(kind,0),value)
        summary["sampled_disk_peaks_bytes"]=disks
        with (self.runner.root/"reports/resources.json").open("w") as out:json.dump(summary,out,indent=2)
