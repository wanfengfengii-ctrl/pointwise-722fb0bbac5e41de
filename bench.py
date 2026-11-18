"""Public whole-call memory benchmark and small independent business checks."""
import argparse
import ast
import gc
import hashlib
import json
from pathlib import Path
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import tracemalloc
import unittest
from warehouse import Warehouse, DataError, NotReady
from warehouse.db import canonical

MIB = 1024 * 1024
WRITER = {"type": "record", "name": "lake.Row", "fields": [
    {"name": "trash", "type": {"type": "array", "items": {
        "type": "record", "name": "Node", "fields": [
            {"name": "text", "type": "string"},
            {"name": "v", "type": ["long", "double", "null"]},
            {"name": "kids", "type": {"type": "array", "items": "Node"}}]}}},
    {"name": "b", "type": "string"}, {"name": "a", "type": "long"}]}
READER = {"type": "record", "name": "view.Row", "aliases": ["lake.Row"], "fields": [
    {"name": "a", "type": "double"}, {"name": "b", "type": "string"},
    {"name": "quality", "type": ["null", "string"], "default": None}]}
NODE = ["x" * 1000, [1, 7.0], []]
SMALL = {"a": 7.0, "b": "河\u0000😀",
         "quality": {"branch": "null", "index": 0, "value": None}}


def escaped(text):
    return json.dumps(text, ensure_ascii=True)[1:-1].encode("ascii")


def parts(kind, size):
    # Produce the actual old outer JSON array of SDK1 JSON strings.
    # The generator builds data only: it is not a parser or a target executor.
    if kind == "projection":
        unit = escaped(canonical(NODE))
        head = b'["' + escaped('{"sdk":1,"datum":[[')
        tail = escaped('],"河\\u0000😀",7]}') + b'"]'
        count = max(1, (size - len(head) - len(tail) + 1) // (len(unit) + 1))
        yield head
        for i in range(count):
            if i:
                yield b","
            yield unit
        yield tail
    else:
        payload = canonical({"sdk": 1, "datum": [[], "y" * 65536, 7]})
        unit = json.dumps(payload, ensure_ascii=True).encode("ascii")
        count = max(1, (size - 1) // (len(unit) + 1))
        yield b"["
        for i in range(count):
            if i:
                yield b","
            yield unit
        yield b"]"


def build(path, kind, size):
    with Warehouse(str(path)) as w:
        wid = w.register_schema(WRITER, "writer")
        rid = w.register_schema(READER, "reader")
        if kind == "late":
            bid = w.register_schema("double", "bad-writer")
            w.publish_batch("bad", bid, [7.0], "bad")
        length = sum(map(len, parts(kind, size)))
        with w.db.transaction():
            cur = w.db.conn.execute(
                "INSERT INTO batches VALUES(?,?,CAST(zeroblob(?) AS TEXT))",
                ("data", wid, length))
            with w.db.conn.blobopen("batches", "payloads", cur.lastrowid) as blob:
                for part in parts(kind, size):
                    blob.write(part)
            inputs = ["data", "bad"] if kind == "late" else ["data"]
            w.db.conn.execute("INSERT INTO jobs VALUES(?,?,?,'queued')",
                              ("job", rid, canonical(inputs)))
        return length


def run(w, backend, job="job"):
    if backend == "reference":
        return w.jobs.materialized_run_job(job)
    return w.run_job(job)


def expected(result, kind):
    if kind == "projection":
        assert result == {"job_id": "job", "state": "done", "rows": [
            {"batch_id": "data", "row_index": 0, "value": SMALL}]}, result
        assert type(result["rows"][0]["value"]["a"]) is float
    else:
        assert result == {"job_id": "job", "state": "failed", "rows": [],
                          "error": {"code": "ResolutionError",
                                    "message": "incompatible writer and reader"}}, result


def worker(path, kind, backend):
    with Warehouse(path) as w:
        assert w.jobs.binding("job")["state"] == "queued"
        gc.collect()
        tracemalloc.start()
        start = time.perf_counter()
        result = run(w, backend)
        elapsed = time.perf_counter() - start
        peak = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()
        expected(result, kind)
        assert result == w.get_result("job")
        if kind == "late":
            assert w.db.conn.execute("SELECT count(*) FROM result_values").fetchone()[0] == 0
    with Warehouse(path) as w:
        expected(w.get_result("job"), kind)
    print(json.dumps({"kind": kind, "backend": backend, "peak": peak,
                      "seconds": elapsed}, sort_keys=True))


def benchmark(mode):
    check_frozen()
    backend = "reference" if mode == "baseline" else "target"
    records = []
    with tempfile.TemporaryDirectory(prefix="warehouse-memory-") as directory:
        root = Path(directory)
        for kind in ("projection", "late"):
            medians = []
            for size in (32 * MIB, 128 * MIB):
                fixture = root / "fixture.sqlite"
                if fixture.exists():
                    fixture.unlink()
                length = build(fixture, kind, size)
                assert size * 0.99 <= length <= size
                peaks = []
                for round_number in range(3):
                    path = root / "run.sqlite"
                    shutil.copyfile(fixture, path)
                    process = subprocess.run(
                        [sys.executable, "-B", str(Path(__file__).resolve()),
                         "worker", "--database", str(path), "--kind", kind,
                         "--backend", backend], check=True, text=True, capture_output=True)
                    record = json.loads(process.stdout)
                    record.update(size=size, payload_bytes=length, round=round_number + 1)
                    records.append(record)
                    peaks.append(record["peak"])
                    print(json.dumps(record, sort_keys=True), flush=True)
                medians.append(statistics.median(peaks))
                if mode == "target":
                    assert max(peaks) <= 24 * MIB, (kind, size, peaks)
            if mode == "target":
                assert medians[1] - medians[0] <= 4 * MIB, (kind, medians)
    return records


def fixture(w, corrupt=False, missing=False):
    reader = json.loads(canonical(READER))
    if missing:
        reader["fields"].insert(0, {"name": "absent", "type": "long"})
    wid = w.register_schema(WRITER, "w")
    rid = w.register_schema(reader, "r")
    node = {"text": "河\u0000😀", "v": 7.0, "kids": [
        {"text": "child", "v": 7, "kids": []}]}
    w.publish_batch("data", wid, [{"trash": [node], "b": SMALL["b"], "a": 7}], "p")
    w.create_job("job", ["data", "data"], rid, "j")
    if corrupt:
        # A dedicated corrupt-old-store fixture; original public APIs stay immutable.
        payload = canonical({"sdk": 1, "datum": [[["ok", [0, True], []]], SMALL["b"], 7]})
        w.db.conn.execute("DROP TRIGGER immutable_batch")
        w.db.conn.execute("UPDATE batches SET payloads=? WHERE batch_id='data'",
                          (canonical([payload]),))


class BusinessChecks(unittest.TestCase):
    def test_projection_repeated_coordinates_and_order(self):
        for backend in ("reference", "target"):
            with Warehouse() as w:
                fixture(w)
                result = run(w, backend)
                self.assertEqual([{"batch_id": "data", "row_index": 0, "value": SMALL}] * 2,
                                 result["rows"])
                self.assertIs(type(result["rows"][0]["value"]["a"]), float)
                self.assertEqual(result, w.get_result("job"))

    def test_dropped_corruption_precedes_reader_error(self):
        for backend in ("reference", "target"):
            with Warehouse() as w:
                fixture(w, corrupt=True, missing=True)
                with self.assertRaises(DataError):
                    run(w, backend)
                self.assertEqual("queued", w.jobs.binding("job")["state"])
                self.assertEqual(0, w.db.conn.execute("SELECT count(*) FROM results").fetchone()[0])
                self.assertEqual(0, w.db.conn.execute("SELECT count(*) FROM result_values").fetchone()[0])

    def test_reader_first_error(self):
        for backend in ("reference", "target"):
            with Warehouse() as w:
                fixture(w, missing=True)
                result = run(w, backend)
                self.assertEqual("missing reader field: absent", result["error"]["message"])
                self.assertEqual([], result["rows"])

    def test_late_errors_discard_prefix(self):
        for backend in ("reference", "target"):
            for failure in ("resolution", "corruption"):
                with Warehouse() as w:
                    wid = w.register_schema(WRITER, "w")
                    rid = w.register_schema(READER, "r")
                    value = {"trash": [{"text": "discard", "v": 7.0, "kids": []}],
                             "b": "output" * 1024, "a": 7}
                    w.publish_batch("prefix", wid, [value] * 8, "prefix")
                    bad_id = w.register_schema("double", "bad") if failure == "resolution" else wid
                    w.publish_batch("tail", bad_id,
                                    [7.0] if failure == "resolution" else [value], "tail")
                    w.create_job("job", ["prefix", "tail"], rid, "j")
                    if failure == "corruption":
                        original = w.archive.read_batch("tail")["payloads"]
                        damaged = canonical({"sdk": 1, "datum": [
                            [["discard", [0, True], []]], "tail", 7]})
                        w.db.conn.execute("DROP TRIGGER immutable_batch")
                        w.db.conn.execute("UPDATE batches SET payloads=? WHERE batch_id='tail'",
                                          (canonical([damaged]),))
                        with self.assertRaises(DataError):
                            run(w, backend)
                        self.assertEqual("queued", w.jobs.binding("job")["state"])
                        with self.assertRaises(NotReady):
                            w.get_result("job")
                        self.assertEqual(0, w.db.conn.execute("SELECT count(*) FROM results").fetchone()[0])
                        self.assertEqual(0, w.db.conn.execute("SELECT count(*) FROM result_values").fetchone()[0])
                        w.db.conn.execute("UPDATE batches SET payloads=? WHERE batch_id='tail'",
                                          (canonical(original),))
                        self.assertEqual(9, len(run(w, backend)["rows"]))
                    else:
                        result = run(w, backend)
                        self.assertEqual("failed", result["state"])
                        self.assertEqual([], result["rows"])
                        self.assertEqual("incompatible writer and reader", result["error"]["message"])
                        self.assertEqual(0, w.db.conn.execute("SELECT count(*) FROM result_values").fetchone()[0])

    def test_hooks_rollback_and_retry(self):
        for backend in ("reference", "target"):
            for point in ("run.after_resolve", "run.before_commit"):
                with Warehouse() as w:
                    fixture(w)
                    def hook(boundary):
                        if boundary == point:
                            raise OSError("injected publication refusal")
                    w.db.hook = hook
                    with self.assertRaisesRegex(OSError, "injected"):
                        run(w, backend)
                    self.assertEqual("queued", w.jobs.binding("job")["state"])
                    self.assertEqual(0, w.db.conn.execute("SELECT count(*) FROM results").fetchone()[0])
                    self.assertEqual(0, w.db.conn.execute("SELECT count(*) FROM result_values").fetchone()[0])
                    w.db.hook = None
                    self.assertEqual("done", run(w, backend)["state"])


def check_frozen():
    root = Path(__file__).resolve().parent
    manifest = json.loads((root / "memory_frozen.json").read_text())
    for name, digest in manifest["files"].items():
        assert hashlib.sha256((root / name).read_bytes()).hexdigest() == digest, name
    for location, digest in manifest["methods"].items():
        name, qualified = location.split(":")
        class_name, method = qualified.split(".")
        tree = ast.parse((root / name).read_text())
        node = next(n for c in tree.body if isinstance(c, ast.ClassDef) and c.name == class_name
                    for n in c.body if isinstance(n, ast.FunctionDef) and n.name == method)
        actual = hashlib.sha256(ast.dump(node, include_attributes=False).encode()).hexdigest()
        assert actual == digest, location


def checks():
    check_frozen()
    suite = unittest.TestSuite([
        unittest.defaultTestLoader.discover("tests", top_level_dir="."),
        unittest.defaultTestLoader.loadTestsFromTestCase(BusinessChecks)])
    if not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful():
        raise SystemExit(1)


def demo():
    with tempfile.TemporaryDirectory(prefix="warehouse-memory-demo-") as directory:
        for kind in ("projection", "late"):
            path = Path(directory) / (kind + ".sqlite")
            build(path, kind, MIB)
            with Warehouse(str(path)) as w:
                result = w.run_job("job")
                expected(result, kind)
                print(json.dumps(result, ensure_ascii=False))
            with Warehouse(str(path)) as w:
                expected(w.get_result("job"), kind)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["baseline", "target", "check", "verify", "demo", "worker"])
    parser.add_argument("--database")
    parser.add_argument("--kind", choices=["projection", "late"])
    parser.add_argument("--backend", choices=["reference", "target"])
    args = parser.parse_args()
    if args.mode == "worker":
        worker(args.database, args.kind, args.backend)
    elif args.mode in ("baseline", "target"):
        benchmark(args.mode)
    elif args.mode == "check":
        checks()
    elif args.mode == "verify":
        checks()
        benchmark("target")
    else:
        demo()


if __name__ == "__main__":
    main()
