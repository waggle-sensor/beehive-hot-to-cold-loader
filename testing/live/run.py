#!/usr/bin/env python3
"""Isolated real-Pelican live gate. No existing stack or remote prefix is reused."""
import argparse
import hashlib
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import sys
from urllib.parse import urlsplit
import uuid

HERE = Path(__file__).resolve().parent
ARCHIVER = HERE.parents[1]
WORKSPACE = ARCHIVER.parent


def local(path):
    path = Path(path).expanduser().absolute()
    if "node-data" in path.parts:
        raise ValueError("/node-data is prohibited")
    path = path.resolve()
    if "node-data" in path.parts:
        raise ValueError("/node-data is prohibited")
    return path


def origin_url(text):
    url = urlsplit(text)
    if (url.scheme != "https" or not url.netloc or url.username or url.password
            or url.path != "/sage" or url.query or url.fragment or "%" in url.netloc):
        raise ValueError("origin must be https://host[:port]/sage")
    return text


def save(path, value):
    temporary = path.with_suffix(".pending")
    with open(temporary, "w", encoding="utf-8", opener=lambda p, f: os.open(p, f, 0o600)) as out:
        json.dump(value, out, indent=2)
        out.write("\n")
        out.flush()
        os.fsync(out.fileno())
    temporary.replace(path)


def copy_source(source, destination, names):
    destination.mkdir()
    for name in names:
        item = source / name
        if item.is_dir():
            shutil.copytree(item, destination / name, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        else:
            shutil.copy2(item, destination / name)


def tree_hash(root):
    digest=hashlib.sha256()
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        digest.update(str(path.relative_to(root)).encode()+b"\0")
        with path.open("rb") as source:
            for block in iter(lambda:source.read(1<<20),b""):
                digest.update(block)
    return digest.hexdigest()


def image_ref(value):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,200}@sha256:[a-f0-9]{64}", value):
        raise ValueError("pinned image must be name@sha256:<64 hex>")
    return value


def pin_application_images(path):
    # The copied compose is private to this run. Removing build blocks keeps
    # compose from replacing the pinned digests with a workspace build.
    text = path.read_text()
    replacements = (
        ("  archiver:\n    build:\n      context: ${ARCHIVER_SOURCE:?}\n",
         "  archiver:\n    image: ${ARCHIVER_IMAGE:?}\n    platform: linux/amd64\n    pull_policy: never\n"),
        ("  loader:\n    build:\n      context: ${LIVE_ROOT:?}/loader-build\n",
         "  loader:\n    image: ${LOADER_IMAGE:?}\n    platform: linux/amd64\n    pull_policy: never\n"),
        ("  api:\n    build:\n      context: ${API_SOURCE:?}\n",
         "  api:\n    image: ${API_IMAGE:?}\n    platform: linux/amd64\n    pull_policy: never\n"),
    )
    for old, new in replacements:
        if old not in text:
            raise ValueError("live compose has no replaceable build block")
        text = text.replace(old, new, 1)
    path.write_text(text)


def prepare(args):
    key = local(args.pelican_key)
    duck = local(args.duckdb)
    if not key.is_file() or not duck.is_file():
        raise ValueError("an existing signing key and architecture-matching DuckDB 2.0 CLI are required")
    run_id = uuid.uuid4().hex
    root = local(args.run_dir or ARCHIVER / "state" / ("live-" + run_id))
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    for name in ("influx", "coordinator", "reports"):
        (root / name).mkdir(mode=0o700)
    # Curated snapshots exclude .git, datasets, state, caches and signing keys.
    copy_source(ARCHIVER, root / "archiver-build", ["go.mod", "go.sum", "main.go", "internal", "Dockerfile"])
    copy_source(WORKSPACE / "beehive-data-api", root / "api-build", ["go.mod", "go.sum", "main.go", "internal", "Dockerfile"])
    copy_source(WORKSPACE / "beehive-influxdb-loader", root / "loader-build", ["requirements.txt", "main.py", "archive.py"])
    shutil.copy2(HERE / "Dockerfile.loader", root / "loader-build/Dockerfile")
    (root / "probe-build").mkdir()
    copy_source(WORKSPACE / "sage-data-client", root / "probe-build/client", ["src", "pyproject.toml", "setup.cfg", "README.md"])
    for source, name in ((HERE / "Dockerfile.probe", "Dockerfile"), (HERE / "probe.py", "probe.py"),
                         (WORKSPACE / "beehive-influxdb-loader/requirements.txt", "requirements.txt"), (duck, "duckdb")):
        shutil.copy2(source, root / "probe-build" / name)
    shutil.copy2(HERE / "compose.yml", root / "compose.yml")
    images = {}
    supplied = (args.archiver_image, args.api_image, args.loader_image)
    if any(supplied):
        if not all(supplied):
            raise ValueError("pin the archiver, API, and loader images together")
        images = {"archiver": image_ref(args.archiver_image), "api": image_ref(args.api_image), "loader": image_ref(args.loader_image)}
        pin_application_images(root / "compose.yml")
    origin = origin_url(args.origin)
    config = {"RUN_ID": run_id, "LIVE_ROOT": str(root), "PELICAN_KEY": str(key),
              "WAREHOUSE": origin + "/cold-storage-archive-live-" + run_id + "/warehouse",
              "ARCHIVER_SOURCE": str(root / "archiver-build"), "API_SOURCE": str(root / "api-build"),
              "ARCHIVER_IMAGE": images.get("archiver", ""), "API_IMAGE": images.get("api", ""),
              "LOADER_IMAGE": images.get("loader", ""),
              "COORDINATE_ONLY": "true", "DELETE_ENABLED": "false"}
    for name in ("LOADER_TOKEN", "READER_TOKEN", "ADMIN_TOKEN", "INFLUX_TOKEN", "INFLUX_PASSWORD", "BROKER_PASSWORD"):
        config[name] = secrets.token_hex(32)
    save(root / "private-config.json", config)
    manifest = {"project": "sage-archive-live-" + run_id, "warehouse": config["WAREHOUSE"],
                "origin": origin, "run_id": run_id, "phase": "prepared", "passed": False,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "execution_policy":{"normalization_threads":16,"normalization_merge_fan_in":16,"normalization_merge_concurrency":1,"comparison_threads":16,"build_threads":16,"build_sort_threads":2,"canonical_reuse":True,"build_canonicalization":"tag_dictionary","reconciliation_canonicalization":"raw_tag_dictionary","verification_canonicalization":"local_dictionary","partitioning":"fixed"},
                "images": images,
                "source_sha256":{name:tree_hash(root/name) for name in ("archiver-build","api-build","loader-build","probe-build")}}
    save(root / "run.json", manifest)
    return root


class Run:
    def __init__(self, root):
        self.root = local(root)
        self.config = json.loads((self.root / "private-config.json").read_text())
        self.manifest = json.loads((self.root / "run.json").read_text())
        run_id = self.config["RUN_ID"]
        if not re.fullmatch("[a-f0-9]{32}", run_id):
            raise ValueError("invalid isolated run ID")
        expected = origin_url(self.manifest["origin"]) + "/cold-storage-archive-live-" + run_id + "/warehouse"
        if (self.config["WAREHOUSE"] != expected or self.manifest["warehouse"] != expected
                or self.manifest["project"] != "sage-archive-live-" + run_id
                or self.config["LIVE_ROOT"] != str(self.root)):
            raise ValueError("run identity/prefix mismatch")
        self.command = ["docker", "compose", "--env-file", os.devnull, "--project-name", self.manifest["project"],
                        "--file", str(self.root / "compose.yml")]

    def compose(self, *args):
        # No shell interpolation and no secrets on the command line.
        subprocess.run(self.command + list(args), env={**os.environ, **self.config}, check=True)

    def phase(self, label):
        self.manifest["phase"] = label
        save(self.root / "run.json", self.manifest)
        print(label, flush=True)

    def probe(self, stage):
        self.phase(stage)
        self.compose("run", "--rm", "--no-deps", "probe", stage)

    def archiver(self, coordinate, deletion=False):
        self.config.update(COORDINATE_ONLY=str(coordinate).lower(), DELETE_ENABLED=str(deletion).lower())
        save(self.root / "private-config.json", self.config)
        self.compose("up", "-d", "--no-deps", "--force-recreate", "archiver")
        self.compose("run", "--rm", "--no-deps", "probe", "ready")

    def execute(self):
        if self.manifest["phase"] != "prepared":
            raise ValueError("only a prepared, never-started run can execute; use a fresh run after failure")
        # Reject before builds/service mutations if this session cannot use Docker.
        subprocess.run(["docker", "info", "--format", "{{.OSType}}/{{.Architecture}}"], check=True)
        started = False
        try:
            images = self.manifest.get("images") or {}
            self.phase("pulling" if images else "building")
            self.compose("config", "--quiet")
            if images:
                for ref in images.values():
                    subprocess.run(["docker", "pull", "--platform", "linux/amd64", ref], check=True)
                    inspected = subprocess.run(["docker", "image", "inspect", ref, "--format", "{{.Os}}/{{.Architecture}}"],
                                               check=True, capture_output=True, text=True)
                    if inspected.stdout.strip() != "linux/amd64":
                        raise ValueError("pinned image is not linux/amd64")
                self.manifest["image_architecture"] = "linux/amd64"
                save(self.root / "run.json", self.manifest)
                self.compose("build", "probe")
            else:
                self.compose("build", "archiver", "api", "loader", "probe")
            started = True
            self.compose("up", "-d", "--wait", "influx", "broker")
            self.probe("bootstrap")
            initial = json.loads((self.root / "reports/bootstrap.json").read_text())
            self.config["BUCKET_ID"] = initial["bucket_id"]
            self.archiver(coordinate=True)
            self.compose("up", "-d", "--no-deps", "loader", "api")
            self.probe("seed")
            self.archiver(coordinate=False)
            self.probe("fenced")
            # The manual write permit keeps the job fenced. Both that permit and
            # the acknowledged late receipt must survive a coordinator restart.
            self.archiver(coordinate=False)
            self.probe("recover-and-release")
            self.probe("archive-and-pin")
            self.probe("fenced-late-published")
            self.compose("stop", "loader")
            self.probe("publish-redelivery")
            self.compose("up", "-d", "--no-deps", "loader")
            self.probe("late-corrected")
            # Only this private bucket/stack participates; all its readers/writers
            # are coordinated. The held old hot read must prevent deletion.
            self.archiver(coordinate=False, deletion=True)
            self.probe("deletion-blocked")
            self.probe("release-and-finish")
            self.manifest["passed"] = True
            self.phase("complete")
        except BaseException as error:
            self.manifest["failed_phase"] = self.manifest["phase"]
            self.manifest["phase"] = "failed"
            self.manifest["error_type"] = type(error).__name__
            save(self.root / "run.json", self.manifest)
            raise
        finally:
            if started:
                try:
                    self.compose("stop")  # Retain state/reports/containers; never delete remote objects.
                    self.manifest["services_stopped"] = True
                except BaseException as error:
                    self.manifest.update(passed=False, phase="failed-stop", teardown_error_type=type(error).__name__)
                    raise
                finally:
                    save(self.root / "run.json", self.manifest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare-only", action="store_true", help="snapshot sources/configuration without Docker or network IO")
    parser.add_argument("--prepared", type=Path, help="execute an existing never-started prepared run")
    parser.add_argument("--stop", type=Path, help="stop only the recorded isolated project")
    parser.add_argument("--run-dir", type=Path)
    parser.add_argument("--origin", default="https://nrdstor.nationalresearchplatform.org:8443/sage")
    parser.add_argument("--pelican-key", type=Path, default=WORKSPACE / "honeyhouse-config/applications/waggle-access/pelican/issuer-key.pem")
    parser.add_argument("--duckdb", type=Path, default=ARCHIVER / "bin/duckdb")
    parser.add_argument("--archiver-image", help="pinned archiver image, name@sha256:<digest>; requires the API and loader pins")
    parser.add_argument("--api-image", help="pinned API image, name@sha256:<digest>")
    parser.add_argument("--loader-image", help="pinned loader image, name@sha256:<digest>")
    args = parser.parse_args()
    if args.stop:
        Run(args.stop).compose("stop")
        return
    if args.prepared and (args.prepare_only or args.run_dir):
        parser.error("--prepared cannot be combined with --prepare-only or --run-dir")
    root = args.prepared or prepare(args)
    print("Private run directory:", root, flush=True)
    if not args.prepare_only:
        Run(root).execute()
    else:
        print("Prepared only; no Docker, Influx, broker or Pelican operations performed.")


if __name__ == "__main__":
    main()
