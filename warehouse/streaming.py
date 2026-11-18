"""Fixed-reader streaming run path with bounded Python intermediate memory.

This module is the optimized companion of ``Jobs.materialized_run_job``.  It
keeps the exact SDK1 write-wire validation semantics (including fields the
reader discards) and the exact writer-to-reader resolution semantics of
``warehouse.legacy``, but never materializes a whole batch or a whole huge row
in Python:

* Batch payload lists are read through a streaming ``json_each`` cursor.  A
  ``CASE`` expression returns the payload text only while it is small and NULL
  for oversize payloads, so a huge row never crosses into Python.
* Small payloads take the frozen pure-Python SDK1/legacy fast path.
* Oversize payloads stay inside SQLite.  One streaming ``json_tree`` scan
  validates the whole write wire (envelope, union indices, every primitive and
  every discarded field) with an explicit schema stack; large text atoms are
  excluded from the scan and lone surrogates are detected by a SQL-only byte
  pass, so neither large strings nor large atoms enter Python in one piece.
  Resolution then walks JSON paths and materializes only reader subtrees that
  survive projection.
* Accepted output rows are handed straight to a revocable internal staging
  table; no Python list accumulates the successful prefix.

Only the Python standard library and the connection's built-in SQLite JSON1
support are used.  No persisted payload format changes; nothing is moved into
another process.
"""
import math

from vendor import sdk1
from vendor.codec import default_wire
from .errors import DataError, ResolutionError
from .legacy import matches, _default, _resolve
from .schemas import MISSING

# Payloads at or below this byte size take the frozen pure-Python fast path.
# It stays a small fraction of the 24 MiB Python peak budget even when several
# transient decoder objects overlap.
INLINE_LIMIT = 512 * 1024
LONG_MIN, LONG_MAX = -(2 ** 63), 2 ** 63 - 1

# SQLite JSON1 raises OperationalError for malformed documents, invalid paths
# or text it cannot decode to the connection encoding.  On the payload path
# those conditions are stored-payload corruption and must surface as DataError,
# never masquerade as an infrastructure/driver failure.
_JSON_ERROR_MARKERS = (
    "malformed JSON",
    "json_each",
    "json_tree",
    "json_extract",
    "json_type",
    "json_array_length",
    "Could not decode to UTF-8",
    "JSON path",
)


def _json_error(exc):
    message = str(exc)
    return isinstance(exc, Exception) and any(m in message for m in _JSON_ERROR_MARKERS)


def _path(steps):
    out = ["$"]
    for step in steps:
        if isinstance(step, int):
            out.append("[" + str(step) + "]")
        else:
            out.append("." + step)
    return "".join(out)


_SURROGATE_TOKENS = tuple(
    "ED" + hi + lo for hi in ("A", "B")
    for lo in ("0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B",
               "C", "D", "E", "F"))


def _surrogate_scan_sql(element_expr):
    # Returns 1 when any text node contains CESU-8 lone-surrogate bytes
    # (ED A0..BF / ED B0..BF).  Matching on the two-character hex token means
    # the marker cannot straddle the hex() grouping the way a one-nibble
    # instr() test could.  Only an integer leaves SQLite, so a huge text value
    # is never transferred into Python.
    predicates = " OR ".join("h LIKE '%" + token + "%'"
                             for token in _SURROGATE_TOKENS)
    return (
        "SELECT COALESCE(max(CASE WHEN (" + predicates
        + ") THEN 1 ELSE 0 END),0) FROM (SELECT hex(cast(value as blob)) AS h "
        "FROM json_tree(" + element_expr + ") WHERE type='text')")


class StoredSource:
    """One oversize payload addressed inside the batches.payloads column.

    ``element_path`` (e.g. ``$[17]``) selects one element of the batch's outer
    JSON array.  All datum paths are expressed relative to that element, with
    an empty step list (``$``) denoting the envelope itself.  The full payload
    text is never bound as a Python parameter.
    """

    def __init__(self, conn, batch_id, element_path):
        self.conn = conn
        self.batch_id = batch_id
        # element_path selects the envelope string inside the batch column,
        # e.g. "$[17]".  Datum steps are relative to the envelope's "datum"
        # member; an empty step list denotes that datum itself.
        self.element_path = element_path

    def _column(self):
        return "(SELECT payloads FROM batches WHERE batch_id=?)"

    def _relative(self, steps):
        # The inner json_extract parses the envelope element; datum content
        # always lives under its "datum" member.
        return "$.datum" + _path(steps)[1:]

    def _params(self, steps):
        return (self.batch_id, self.element_path, self._relative(steps))

    def _element_expr(self):
        return "json_extract(" + self._column() + ",?)"

    # SQLite JSON1 truncates a materialized JSON string at an embedded NUL,
    # even though the stored escape is intact.  Rewriting the six-character
    # escape to this private-use sentinel before extraction (and restoring the
    # NUL in Python afterwards) preserves the exact SDK1 string contents.
    _NUL_ESCAPE = "\\u0000"
    _NUL_SENTINEL = ""

    def _nul_safe_envelope(self):
        # Parsed envelope text with every NUL escape swapped for the sentinel.
        return ("replace(" + self._element_expr() + ",'" + self._NUL_ESCAPE
                + "',?)")

    def type_at(self, steps):
        try:
            return self.conn.execute(
                "SELECT json_type(" + self._element_expr() + ",?)",
                self._params(steps)).fetchone()[0]
        except Exception as exc:
            if _json_error(exc):
                raise DataError("invalid SDK1 payload") from exc
            raise

    def length_at(self, steps):
        try:
            return self.conn.execute(
                "SELECT json_array_length(" + self._element_expr() + ",?)",
                self._params(steps)).fetchone()[0]
        except Exception as exc:
            if _json_error(exc):
                raise DataError("invalid SDK1 payload") from exc
            raise

    def _extract(self, steps):
        return self.conn.execute(
            "SELECT json_extract(" + self._element_expr() + ",?)",
            self._params(steps)).fetchone()[0]

    def _extract_text(self, steps):
        # Extract through the NUL-sanitized envelope, then restore NULs.  A
        # lone surrogate still fails the driver decode (the structural
        # validator's surrogate scan already rejects it first).
        try:
            value = self.conn.execute(
                "SELECT json_extract(" + self._nul_safe_envelope() + ",?)",
                (self.batch_id, self.element_path, self._NUL_SENTINEL,
                 self._relative(steps))).fetchone()[0]
        except Exception as exc:
            if _json_error(exc):
                raise DataError("invalid SDK1 payload") from exc
            raise
        if type(value) is not str:
            raise DataError("value does not match schema")
        return value.replace(self._NUL_SENTINEL, "\x00")

    def scalar_at(self, steps, jtype):
        # Inspect the declared JSON type first.  When the envelope column has
        # TEXT storage class, json_extract returns an integer-valued number as
        # a Python float, which would erase the long/double distinction; parse
        # integers from the exact digits whenever the JSON type says integer.
        declared = self.type_at(steps)
        if jtype == "null":
            if declared != "null":
                raise DataError("value does not match schema")
            return None
        if jtype == "string":
            if declared != "text":
                raise DataError("value does not match schema")
            return self._extract_text(steps)
        try:
            if declared == "integer":
                text = self._extract(steps)
                value = text if type(text) is int else int(text)
            elif declared == "real":
                value = self._extract(steps)
            else:
                value = self._extract(steps)
        except DataError:
            raise
        except Exception as exc:
            if _json_error(exc):
                raise DataError("invalid SDK1 payload") from exc
            raise
        if jtype == "long":
            if declared != "integer" or type(value) is not int \
                    or not LONG_MIN <= value <= LONG_MAX:
                raise DataError("value does not match schema")
            return value
        if jtype == "double":
            if type(value) not in (int, float):
                raise DataError("value does not match schema")
            number = float(value)
            if not math.isfinite(number):
                raise DataError("value does not match schema")
            return number
        raise DataError("value does not match schema")

    def cursor(self, steps):
        """Yield (index, json_type, serialized_value_length) in stored order."""
        try:
            cursor = self.conn.execute(
                "SELECT key,type,length(value) FROM json_each("
                + self._element_expr() + ",?)", self._params(steps))
        except Exception as exc:
            if _json_error(exc):
                raise DataError("invalid SDK1 payload") from exc
            raise
        while True:
            try:
                row = cursor.fetchone()
            except Exception as exc:
                if _json_error(exc):
                    raise DataError("invalid SDK1 payload") from exc
                raise
            if row is None:
                return
            yield int(row[0]), row[1], row[2]


class _Frame:
    """One open JSON container during the single json_tree validation scan.

    ``envelope`` frames validate the two named SDK1 envelope members.  Other
    frames validate record (fixed field count), union (exactly index + value)
    and array (unbounded) write wires.  The walk is purely structural, so the
    frame carries no path (resolution computes its own paths independently).
    """

    __slots__ = ("tid", "node", "seen", "expect", "index", "envelope")

    def __init__(self, tid, node, envelope=False):
        self.tid = tid
        self.node = node
        self.seen = 0
        self.envelope = envelope
        self.index = None
        if envelope:
            self.expect = 2
        elif node is None:
            self.expect = None
        elif node.kind == "record":
            self.expect = len(node.fields)
        elif node.kind == "union":
            self.expect = 2
        else:
            self.expect = None


def _check_leaf(node, jtype, atom):
    kind = node.kind
    if kind == "long":
        if jtype != "integer" or type(atom) is not int or not LONG_MIN <= atom <= LONG_MAX:
            raise DataError("value does not match schema")
    elif kind == "double":
        if jtype != "real" or type(atom) not in (int, float) \
                or not math.isfinite(float(atom)):
            raise DataError("value does not match schema")
    elif kind == "null":
        if jtype != "null":
            raise DataError("value does not match schema")
    elif kind == "string":
        # The scan never materializes a text atom; only its type is available
        # here.  Byte/lone-surrogate validation is the separate SQL scan.
        if jtype != "text":
            raise DataError("value does not match schema")
    else:
        raise DataError("value does not match schema")


def validate_stored(source, writer_root):
    """Validate envelope + full write wire of one oversize payload in one scan.

    A single streaming ``json_tree`` cursor walks the whole document in
    depth-first order.  Only integer/real/null atoms cross into Python (large
    text atoms are excluded by the CASE expression); a second SQL-only pass
    detects lone-surrogate bytes in text nodes without materializing any
    string.  The frame stack mirrors ``vendor.codec.validate_wire`` plus the
    SDK1 envelope check, preserving first-error ordering (record fields, array
    elements, union index before its value).
    """
    conn = source.conn
    # json_tree over the envelope element (the element path selects it inside
    # the batch column).
    element_expr = "json_extract(" + source._column() + ",?)"
    sql = (
        "SELECT id,parent,key,type,"
        "CASE WHEN type IN ('integer','real','null') THEN atom ELSE NULL END "
        "FROM json_tree(" + element_expr + ")")
    try:
        cursor = conn.execute(sql, (source.batch_id, source.element_path))
    except Exception as exc:
        if _json_error(exc):
            raise DataError("invalid SDK1 payload") from exc
        raise

    stack = []
    saw_root = False
    while True:
        try:
            row = cursor.fetchone()
        except Exception as exc:
            if _json_error(exc):
                raise DataError("invalid SDK1 payload") from exc
            raise
        if row is None:
            break
        tid, parent, key, jtype, atom = row

        if not saw_root:
            saw_root = True
            if jtype != "object":
                raise DataError("invalid SDK1 payload")
            stack.append(_Frame(tid, None, envelope=True))
            continue

        while stack and stack[-1].tid != parent:
            closed = stack.pop()
            if closed.expect is not None and closed.seen != closed.expect:
                raise DataError("value does not match schema")
        if not stack:
            raise DataError("invalid SDK1 payload")
        frame = stack[-1]

        if frame.envelope:
            position = frame.seen
            frame.seen += 1
            if position == 0:
                if key != "sdk" or jtype != "integer" or atom != 1:
                    raise DataError("invalid SDK1 payload")
                continue
            if position != 1 or key != "datum":
                raise DataError("invalid SDK1 payload")
            if writer_root.kind in ("record", "array", "union"):
                if jtype != "array":
                    raise DataError("value does not match schema")
                stack.append(_Frame(tid, writer_root))
            else:
                _check_leaf(writer_root, jtype, atom)
            continue

        position = frame.seen
        if int(key) != position:
            raise DataError("value does not match schema")
        frame.seen += 1
        node = frame.node

        if node.kind == "record":
            child = node.fields[position][1]
        elif node.kind == "array":
            child = node.items
        elif node.kind == "union":
            if position == 0:
                if jtype != "integer" or type(atom) is not int \
                        or not 0 <= atom < len(node.branches):
                    raise DataError("value does not match schema")
                frame.index = atom
                continue
            child = node.branches[frame.index]
        else:
            raise DataError("value does not match schema")

        if child.kind in ("record", "array", "union"):
            if jtype != "array":
                raise DataError("value does not match schema")
            stack.append(_Frame(tid, child))
        else:
            _check_leaf(child, jtype, atom)

    while stack:
        closed = stack.pop()
        if closed.expect is not None and closed.seen != closed.expect:
            raise DataError("value does not match schema")
    if not saw_root:
        raise DataError("invalid SDK1 payload")
    # Whole-document text validation in one SQL pass: rejects lone surrogates
    # without materializing any string into Python.
    try:
        has_surrogate = conn.execute(
            _surrogate_scan_sql(element_expr),
            (source.batch_id, source.element_path)).fetchone()[0]
    except Exception as exc:
        if _json_error(exc):
            raise DataError("invalid SDK1 payload") from exc
        raise
    if has_surrogate:
        raise DataError("value does not match schema")


def resolve_stream(source, writer, reader, steps=()):
    """Writer-to-reader resolution over a payload kept inside SQLite."""
    if writer.kind == "union":
        index = source.scalar_at(steps + (0,), "long")
        return resolve_stream(source, writer.branches[index], reader,
                              steps + (1,))
    if reader.kind == "union":
        for index, branch in enumerate(reader.branches):
            if matches(writer, branch):
                return {"branch": branch.label, "index": index,
                        "value": resolve_stream(source, writer, branch, steps)}
        raise ResolutionError("no matching reader union branch")
    if not matches(writer, reader):
        raise ResolutionError("incompatible writer and reader")
    if writer.kind == "record":
        positions = {name: position
                     for position, (name, _, _) in enumerate(writer.fields)}
        output = {}
        for name, child, default in reader.fields:
            if name in positions:
                position = positions[name]
                wchild = writer.fields[position][1]
                output[name] = resolve_stream(source, wchild, child,
                                              steps + (position,))
            elif default is not MISSING:
                output[name] = _default(child, default_wire(child, default))
            else:
                raise ResolutionError("missing reader field: " + name)
        return output
    if writer.kind == "array":
        values = []
        for index, _, _ in source.cursor(steps):
            values.append(resolve_stream(source, writer.items, reader.items,
                                         steps + (index,)))
        return values
    if writer.kind == "long" and reader.kind == "double":
        return float(source.scalar_at(steps, "long"))
    return source.scalar_at(steps, writer.kind)


def process_row(conn, batch_id, element_path, byte_length, payload,
                writer_root, reader_root):
    """Validate one full row then resolve it, choosing fast or streaming path.

    Full write-wire validation (envelope plus every field, including fields
    the reader discards) always precedes resolution for the row.  ``payload``
    is the row text when it was small enough to transfer, else None.
    """
    if payload is not None and byte_length <= INLINE_LIMIT:
        # Frozen reference behavior, including envelope and DataError shape.
        wire = sdk1.load_wire(writer_root, payload)
        return _resolve(writer_root, reader_root, wire)
    source = StoredSource(conn, batch_id, element_path)
    validate_stored(source, writer_root)
    return resolve_stream(source, writer_root, reader_root, ())


def batch_payload_cursor(conn, batch_id):
    """Stream (row_index, element_path, byte_length, payload_or_None).

    The batch's outer JSON array of SDK1 payload strings is scanned by SQLite
    itself.  The ``CASE`` expression transfers a payload text only while it is
    at most :data:`INLINE_LIMIT` bytes; an oversize element yields None so the
    huge text never enters Python, while its path still addresses it in place.
    """
    sql = (
        "SELECT e.key, '$[' || e.key || ']', length(e.value), "
        "CASE WHEN length(e.value) <= ? THEN e.value ELSE NULL END "
        "FROM batches AS b, json_each(b.payloads) AS e "
        "WHERE b.batch_id=? ORDER BY e.key")
    try:
        cursor = conn.execute(sql, (INLINE_LIMIT, batch_id))
    except Exception as exc:
        if _json_error(exc):
            raise DataError("invalid SDK1 payload") from exc
        raise
    while True:
        try:
            row = cursor.fetchone()
        except Exception as exc:
            if _json_error(exc):
                raise DataError("invalid SDK1 payload") from exc
            raise
        if row is None:
            return
        yield int(row[0]), row[1], row[2], row[3]
