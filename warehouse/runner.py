"""Streaming execution of fixed-reader jobs with revocable output staging."""
from .db import canonical
from .errors import ResolutionError
from .schemas import Schema
from .streaming import process_row, batch_payload_cursor

# Internal staging table for resolved rows of the run currently in flight.  It
# lives in the same transaction as the job: a successful publication copies the
# rows into result_values and drops the table, a ResolutionError drops the
# staged prefix and publishes only the failure, and every other exception rolls
# the whole transaction back (queued job, no rows) which also removes the
# freshly created staging table.  It has no foreign keys so an uncommitted
# staged prefix never constrains the immutable business tables.
STAGE_DDL = """
CREATE TABLE IF NOT EXISTS _run_staging(
 job_id TEXT NOT NULL, ordinal INTEGER NOT NULL,
 batch_id TEXT NOT NULL, row_index INTEGER NOT NULL, value TEXT NOT NULL)
"""
DROP_STAGE = "DROP TABLE IF EXISTS _run_staging"


class StreamingRunner:
    def __init__(self, jobs):
        self.jobs = jobs
        self.db = jobs.db
        self.schemas = jobs.schemas
        self.results = jobs.results

    def run(self, job_id):
        with self.db.transaction("run"):
            binding = self.jobs.binding(job_id)
            if binding["state"] != "queued":
                # A terminal rerun returns the stored result without staging.
                return self.results.get_result(job_id)
            reader = Schema(self.schemas.get(binding["reader_id"])).root
            self.db.conn.execute(DROP_STAGE)
            self.db.conn.execute(STAGE_DDL)
            ordinal = 0
            try:
                for batch_id in binding["batch_ids"]:
                    writer = self._writer(batch_id)
                    for row_index, element_path, byte_length, payload in \
                            batch_payload_cursor(self.db.conn, batch_id):
                        value = process_row(
                            self.db.conn, batch_id, element_path, byte_length,
                            payload, writer, reader)
                        self.db.conn.execute(
                            "INSERT INTO _run_staging VALUES(?,?,?,?,?)",
                            (job_id, ordinal, batch_id, row_index,
                             canonical(value)))
                        ordinal += 1
                # Same hook point and meaning as the materialized reference:
                # every row resolved, nothing terminal published yet.
                self.db.hit("run.after_resolve")
            except ResolutionError as error:
                # Business failure: only failed + empty rows are published.
                # The staged successful prefix is revoked inside this
                # transaction, so no run leaves staging behind.
                self.db.conn.execute(DROP_STAGE)
                return self.results.publish(job_id, error=error)
            self._publish_done(job_id)
            return self.results.get_result(job_id)

    def _writer(self, batch_id):
        row = self.db.conn.execute(
            "SELECT schema_id FROM batches WHERE batch_id=?",
            (batch_id,)).fetchone()
        if row is None:
            # create_job validated every input; reaching here means store damage.
            from .errors import NotFound
            raise NotFound("unknown batch")
        return Schema(self.schemas.get(row[0])).root

    def _publish_done(self, job_id):
        self.db.conn.execute(
            "INSERT INTO results VALUES(?,'done',NULL,NULL)", (job_id,))
        self.db.conn.execute(
            "INSERT INTO result_values(job_id,ordinal,batch_id,row_index,value) "
            "SELECT job_id,ordinal,batch_id,row_index,value FROM _run_staging "
            "WHERE job_id=? ORDER BY ordinal", (job_id,))
        self.db.conn.execute(
            "UPDATE jobs SET state='done' WHERE job_id=?", (job_id,))
        self.db.conn.execute(DROP_STAGE)
