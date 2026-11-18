# 固定读模式内存优化：表示交接说明

本说明覆盖 `warehouse/streaming.py` 与 `warehouse/runner.py` 的内部表示、
成功前缀归属、晚失败撤销以及成功返回的内存计量口径。所有外部接口、持久化
SDK1 载荷格式、旧 DDL、冻结参照方法与原断言均未改变。

## 1. 可信载荷与字段表示：如何保持写分支身份与读侧访问次序

### 批次读取（不再整批物化）
`batch_payload_cursor` 对 `batches.payloads` 这个“SDK1 JSON 字符串的 JSON
数组”开一个流式 `json_each` 游标。每行只回传
`(row_index, '$[i]', 字节长度, payload或NULL)`；其中 `CASE WHEN length<=
INLINE_LIMIT THEN value ELSE NULL` 保证超大载荷文本根本不进入 Python，
只把它在列内的 JSON 路径 `$[i]` 交给后续处理。行序由 `ORDER BY e.key` 保持，
因此重复批次坐标、`row_index` 与原实现一致。

### 小载荷：冻结纯 Python 快路
字节长度不超过 `INLINE_LIMIT`（512 KiB）的载荷走冻结的 `sdk1.load_wire`
+ `legacy._resolve`，行为与参照逐字节一致（信封校验、long/double 精确类型、
UTF-8、联合索引、首错顺序都由原代码负责）。

### 大载荷：留在 SQLite 内的两路处理
超大载荷用 `StoredSource` 表示，它只持有 `(连接, batch_id, '$[i]')`，
从不绑定整段载荷文本。所有 datum 路径都是相对该信封元素的
`$.datum[...]`，由内层 `json_extract(列, 元素路径)` 先把外层数组元素
（一段 SDK1 信封字符串）解析为 JSON 文档，再按相对路径取值。

* **完整写载荷校验先于解析，且丢弃字段也校验。** `validate_stored` 用单个
  流式 `json_tree` 游标做一次 DFS，配合显式 schema 栈 `_Frame`：
  - record 帧按声明字段数闭合，array 帧不限长度，union 帧固定为
    “索引 + 值”两个元素；联合索引先于其分支值被读取和范围校验，写分支
    身份（writer branch index）因此被原样保留，绝不靠 Python 值猜测。
  - 整数/实数/null 原子通过 `CASE ... atom` 直接取回并按
    long（含 int64 边界、布尔拒绝）、double（有限性）、null 精确校验；
    文本原子被 CASE 排除，不进入 Python。
  - 栈的入栈/闭合顺序与 `vendor.codec.validate_wire` 相同（record 字段序、
    array 元素序、union 索引在前），所以首个错误的位置与冻结校验一致。
* **字符串 UTF-8 / 代理项。** JSON1 自身保证普通字符串内容为合法 UTF-8；
  唯一需要额外拒绝的是 lone surrogate，SQLite 会把它存成 CESU-8 字节
  `ED A0..BF / ED B0..BF`。`_surrogate_scan_sql` 在同一家族的 `json_tree`
  上用 `hex(cast(value as blob))` 配合 32 个精确两字节 token 的 LIKE 判定，
  只回传一个整数标志，任何字符串都不会被整体拉进 Python。
* **NUL 与 long/double 身份的两个驱动层陷阱。**
  - JSON1 在物化字符串时会在内嵌 NUL 处截断。`_extract_text` 先用
    `replace(...,'\u0000', 私用区哨兵 U+E000)` 改写六字符转义再抽取，
    然后在 Python 内把哨兵还原为 `\x00`，从而完整保留 `河\u0000😀` 等值。
  - TEXT 存储列上的 `json_extract` 会把整数值返回成 float，抹掉
    long/double 区分。`scalar_at` 先读声明的 `json_type`，仅当声明为
    integer 时按整数解析，并独立校验 int64 范围；double 才走浮点。
* **读侧解析次序。** `resolve_stream` 是 `legacy._resolve` 的同构移植：
  写联合先取持久化索引再解析实际分支；读联合按声明顺序取第一个
  schema 匹配分支；record 按 reader 字段序访问、同名字段递归、缺字段用
  reader 默认值（联合默认恒为 0 分支）或报 `missing reader field: NAME`；
  array 按元素序；只有 long→double 允许提升。路径化访问天然只物化
  reader 实际保留的子树，被投影丢弃的大字段不生成 Python 对象。

## 2. 成功前缀由谁持有

由 SQLite 内部暂存表 `_run_staging` 持有，而不是 Python 列表。每解析并
接纳一行，就在同一事务内 `INSERT` 一条
`(job_id, ordinal, batch_id, row_index, canonical(value))`。Python 侧任何
时刻只保留当前一行的解析值，这正是峰值不随输入规模增长的原因。该表没有
外键，未提交的暂存前缀不会约束不可变业务表。

## 3. 晚失败如何撤销

* **ResolutionError（业务失败）。** 在 `run` 的 try 内捕获，先
  `DROP TABLE _run_staging` 丢弃已接纳的成功前缀，再调用冻结的
  `Results.publish(error=...)`，只发布 `failed` 与空 `rows`，任务状态置
  failed，同一事务提交。`run.after_resolve` 仅在全部行解析成功后触发，
  语义与参照一致。
* **DataError 及其他异常（含两个钩子）。** 不捕获，直接穿透
  `db.transaction("run")` 的上下文，触发 `conn.rollback()`：任务回到
  queued，results/result_values 无任何写入，而本轮 `CREATE TABLE
  _run_staging` 也随回滚消失，即“新增暂存被释放”。`run.before_commit`
  钩子在提交点抛错时同样走这条回滚路径。成功、失败、异常、重试后均不会
  残留无人需要的暂存表。
* 终态重跑直接返回 `Results.get_result`，不再建暂存表；请求回执、关闭
  重开与固定版本绑定沿用原机制。

## 4. 成功返回的必要结果内存如何计入

基准在子进程内 `tracemalloc.start()` 之后才调用 `run_job`，并在拿到完整
返回值之后才 `get_traced_memory()` 停止，因此解析、暂存写入、终态发布、
事务提交以及**成功返回的结果对象本身**都在计量窗口内，没有外移到准备
阶段、其他进程或测量结束之后。成功路径结束时 `get_result` 构造的 rows
属于完整返回值的必要内存，照常计入；投影负载每行只返回一个小记录，
late 负载成功前缀不落 Python，故实测峰值约 1.15 MiB 且与 32/128 MiB
规模基本无关。SQLite 原生内存不由 tracemalloc 统计，符合“只测 Python
中间分配”的口径。

## 5. 实测结论（逐轮）

`python3.11 -B bench.py target` / `verify`：两类负载、两种规模各三轮，
Python 分配峰值均约 1.15 MiB（上限 24 MiB）；四倍规模的峰值中位增量约
几十字节（上限 4 MiB）。物化参照同口径峰值为 67 MiB–1.06 GiB。
原 25 个回归、bench 的 5 个独立业务断言（含 reference/target 双后端）与
新增 `tests/test_streaming.py` 共 33 个单测全部通过；另有独立差分脚本
逐形状比对 target 与冻结参照（18 个正常/解析失败用例、23 个写载荷损坏
用例），结果完全一致。
