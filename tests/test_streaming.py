"""Streaming run-path checks: staging cleanup, reopen and exception semantics.

These complement the byte-pinned original suite and the bench business checks.
They exercise the optimized Warehouse.run_job path only and never modify the
frozen reference methods or persisted payload format.
"""
import copy
import tempfile
import unittest
from pathlib import Path

from warehouse import Warehouse, DataError, ResolutionError, NotReady
from warehouse.db import canonical
from .support import Case, branch


def staging_exists(w):
    return w.db.conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='_run_staging'"
    ).fetchone() is not None


class StreamingStagingTest(Case):
    def _job(self):
        writer = {"type": "record", "name": "W", "fields": [
            {"name": "id", "type": "long"},
            {"name": "drop", "type": {"type": "array", "items": "string"}}]}
        reader = {"type": "record", "name": "R", "aliases": ["W"], "fields": [
            {"name": "id", "type": "double"},
            {"name": "note", "type": "string", "default": "n"}]}
        wid = self.schema(writer, "w")
        rid = self.schema(reader, "r")
        self.w.publish_batch("b", wid, [
            {"id": 1, "drop": ["a", "b"]},
            {"id": 2, "drop": []}], "p")
        self.w.create_job("job", ["b", "b"], rid, "j")

    def test_success_removes_staging_and_persists_rows(self):
        self._job()
        self.assertFalse(staging_exists(self.w))
        result = self.w.run_job("job")
        self.assertEqual("done", result["state"])
        self.assertEqual(4, len(result["rows"]))
        self.assertFalse(staging_exists(self.w))
        # Terminal rerun neither stages nor rebuilds the table.
        self.assertEqual(result, self.w.run_job("job"))
        self.assertFalse(staging_exists(self.w))

    def test_resolution_error_removes_staging_and_publishes_empty(self):
        self._job()
        bad = self.schema("double", "badw")
        self.w.publish_batch("tail", bad, [7.0], "pt")
        self.w.create_job("late", ["b", "tail"],
                          self.w.jobs.binding("job")["reader_id"], "jl")
        result = self.w.run_job("late")
        self.assertEqual("failed", result["state"])
        self.assertEqual([], result["rows"])
        self.assertEqual("ResolutionError", result["error"]["code"])
        self.assertFalse(staging_exists(self.w))
        self.assertEqual(0, self.w.db.conn.execute(
            "SELECT count(*) FROM result_values").fetchone()[0])

    def test_staging_write_failure_rolls_back_and_frees_staging(self):
        self._job()
        from warehouse import runner as runner_mod
        original = runner_mod.canonical

        def boom(value):
            raise RuntimeError("staging write refused")

        runner_mod.canonical = boom
        try:
            with self.assertRaisesRegex(RuntimeError, "staging write refused"):
                self.w.run_job("job")
        finally:
            runner_mod.canonical = original
        # Non-business exception: job stays queued, no terminal rows, and the
        # freshly created staging table is released by the rollback.
        self.assertEqual("queued", self.w.jobs.binding("job")["state"])
        self.assertEqual(0, self.w.db.conn.execute(
            "SELECT count(*) FROM results").fetchone()[0])
        self.assertEqual(0, self.w.db.conn.execute(
            "SELECT count(*) FROM result_values").fetchone()[0])
        self.assertFalse(staging_exists(self.w))
        # The same job then runs cleanly on retry.
        self.assertEqual("done", self.w.run_job("job")["state"])
        self.assertFalse(staging_exists(self.w))

    def test_hook_rollback_frees_staging_at_both_boundaries(self):
        self._job()
        for point in ("run.after_resolve", "run.before_commit"):
            def hook(boundary, point=point):
                if boundary == point:
                    raise OSError("boundary " + point)
            self.w.db.hook = hook
            with self.assertRaises(OSError):
                self.w.run_job("job")
            self.assertEqual("queued", self.w.jobs.binding("job")["state"])
            self.assertFalse(staging_exists(self.w))
        self.w.db.hook = None
        self.assertEqual("done", self.w.run_job("job")["state"])


class StreamingSemanticsTest(Case):
    def test_independent_name_environments_stay_separate(self):
        # Same short record name in two namespaces must not cross-resolve.
        writer = {"type": "record", "name": "a.Node", "namespace": "a",
                  "fields": [{"name": "v", "type": "long"}]}
        reader_doc = {"type": "record", "name": "b.Node", "namespace": "b",
                      "fields": [{"name": "v", "type": "long"}]}
        wid = self.schema(writer, "w")
        rid = self.schema(reader_doc, "r")
        self.w.publish_batch("b", wid, [{"v": 1}], "p")
        self.w.create_job("job", ["b"], rid, "j")
        result = self.w.run_job("job")
        self.assertEqual("failed", result["state"])
        self.assertEqual("ResolutionError", result["error"]["code"])

    def test_nul_and_unicode_round_trip_through_projection(self):
        writer = {"type": "record", "name": "W", "fields": [
            {"name": "big", "type": {"type": "array", "items": "string"}},
            {"name": "s", "type": "string"}]}
        reader = {"type": "record", "name": "R", "aliases": ["W"], "fields": [
            {"name": "s", "type": "string"}]}
        wid = self.schema(writer, "w")
        rid = self.schema(reader, "r")
        text = "河\u0000😀\t\n\U0010ffff"
        self.w.publish_batch("b", wid, [
            {"big": ["x" * 4096, "y" * 4096], "s": text}], "p")
        self.w.create_job("job", ["b"], rid, "j")
        result = self.w.run_job("job")
        self.assertEqual(text, result["rows"][0]["value"]["s"])
        # Persisted canonical text survives a fresh read too.
        self.assertEqual(text, self.w.get_result("job")["rows"][0]["value"]["s"])

    def test_dropped_field_corruption_precedes_reader_missing_field(self):
        writer = {"type": "record", "name": "W", "fields": [
            {"name": "keep", "type": "long"},
            {"name": "drop", "type": {"type": "array", "items": "string"}}]}
        reader = {"type": "record", "name": "R", "aliases": ["W"], "fields": [
            {"name": "keep", "type": "long"},
            {"name": "absent", "type": "long"}]}
        wid = self.schema(writer, "w")
        rid = self.schema(reader, "r")
        self.w.publish_batch("b", wid, [{"keep": 1, "drop": ["ok"]}], "p")
        self.w.create_job("job", ["b"], rid, "j")
        # Corrupt a field the reader would discard.
        damaged = canonical({"sdk": 1, "datum": [[["ok", 1]], 1]})
        self.w.db.conn.execute("DROP TRIGGER immutable_batch")
        self.w.db.conn.execute("UPDATE batches SET payloads=? WHERE batch_id='b'",
                               (canonical([damaged]),))
        with self.assertRaises(DataError):
            self.w.run_job("job")
        self.assertEqual("queued", self.w.jobs.binding("job")["state"])
        self.assertFalse(staging_exists(self.w))


class StreamingReopenTest(unittest.TestCase):
    def test_success_and_failure_results_survive_reopen(self):
        with tempfile.TemporaryDirectory(prefix="warehouse-stream-") as directory:
            path = str(Path(directory) / "w.sqlite")
            with Warehouse(path) as w:
                long_id = w.register_schema("long", "w")
                double_id = w.register_schema("double", "r")
                w.publish_batch("ok", long_id, [1, 2], "pok")
                w.create_job("good", ["ok", "ok"], double_id, "jg")
                good = w.run_job("good")
                w.publish_batch("bad", double_id, [7.0], "pbad")
                # A long reader cannot consume a double writer: late failure.
                w.create_job("late", ["ok", "bad"], long_id, "jl")
                failed = w.run_job("late")
                self.assertEqual("done", good["state"])
                self.assertEqual("failed", failed["state"])
            with Warehouse(path) as w:
                self.assertEqual(good, w.get_result("good"))
                self.assertEqual(failed, w.get_result("late"))
                self.assertFalse(staging_exists(w))


if __name__ == "__main__":
    unittest.main()
