# 固定读模式加工的中间内存优化

## 业务和范围

归档中一行可能含很大的递归字段，而固定读模式只保留两个小字段；当前实现仍先加载整批、整行。另一些任务产生大量合法输出后，最后一个批次才遇到不兼容模式，最终只发布错误，却已累计全部成功前缀。本题共同目标是降低这两条真实加工链的中间内存，不增加加工能力。

A 负责旧载荷读取、完整写载荷校验和固定读模式解析。B 负责接纳已解析输出、处理其可撤销归属以及终态发布。只完成 A，原 Jobs 的 values 列表仍累计失败前的大输出；只完成 B，Archive 及单行 SDK 解码仍加载巨大被丢弃字段。

所有外部数据、入口、返回格式和原失败语义沿用 CONTRACT.md。优化入口仍是 Warehouse.run_job(job_id)，不得改成流式返回。每行完整写载荷校验先于该行解析，丢弃字段也须校验；解析首错仍按读字段及数组元素顺序决定。不能提前全任务预检而让后续载荷损坏抢在先前 ResolutionError 前面。保持 long/double 的精确类型、字符串 UTF-8 校验、写联合索引、读联合首匹配、递归引用、独立名称环境、读默认值、重复输入的行序和来源坐标。损坏载荷按现有 SDK1 的行为拒绝，不得把数据库驱动异常冒充 DataError。

已完成输出可以暂存，但后续 ResolutionError 只发布 failed 和空 rows；DataError 及其他非业务异常传播并回滚为 queued，新增暂存须释放。成功、业务失败、异常和重试后均不得留下本轮无人需要的暂存。原 run.after_resolve 和 run.before_commit 钩子保持原触发意义及事务边界，终态重跑、请求回执和关闭重开保持兼容。

允许 SQLite JSON 能力、事务、索引、全量扫描、回读及新增内部表示；不要求自建解析器，不限制采用哪一种表示。不得修改旧持久载荷格式，不做 SDK 升级、并发、暂停续跑或新业务接口。

## 正确物化参照

原 Jobs.run_job 的完整方法体只改名为 Jobs.materialized_run_job；run_job 初始仍直接调用它。因此初始任务的业务结果正确，优化尚未实现，没有第二份母项目源码。

bench.py 的 reference 分支调用该物化方法，target 分支调用正式 Warehouse.run_job。memory_frozen.json 固定原测试、参照所用的模式/SDK1/解析代码，以及下列原方法的 AST：
Jobs.materialized_run_job、Jobs.create_job、Jobs.binding、Archive.read_batch、Results.publish、Results.get_result。
这些参照方法和依赖须保持原样，不能通过修改它们改变参照；可以在现有组件或新模块添加正式路径所需的内部能力。JSON 的空白不影响 AST 检查，原测试及所列依赖文件则按字节检查。不得修改 bench.py、memory_frozen.json 的负载、计量、断言或参考分支。

## 可重跑入口

从项目根目录运行，命令均使用 Python 标准库：

- python3.11 -B -m unittest discover -s tests -t . -v：全部原回归。
- python3.11 -B bench.py check：原回归与新增的独立小规模业务断言。
- python3.11 -B bench.py baseline：物化参照的完整资源实测；输出逐轮 JSON 记录，不检查优化阈值。
- python3.11 -B bench.py target：正式路径的完整资源准入。
- python3.11 -B bench.py verify：完整原回归、独立业务检查及正式资源准入。
- python3.11 -B bench.py demo：展示真实投影成功、晚失败和重开结果。

保留现有 .pointwise-runtime.json 的 version、kind 和 setup 格式；交付时 verify 接通 bench.py verify，demo 接通 bench.py demo，两者均使用 argv 数组。

## 可重复资源基准

两类负载各有接近32MiB、128MiB的实际持久 payloads 列：projection 是单行巨大递归数组被丢弃，返回一个小记录；late 是大量合法64KiB输出行后接不兼容批次，返回空 rows 的失败。生成器直接产生 SDK1 的旧双层 JSON 格式；准备和入队不计入执行窗口。它不提供目标解析器、字段索引或结果暂存实现。

基准在独立子进程内打开一个新复制的 queued 仓库，随后开始 tracemalloc 和计时，完整执行一次 run_job。完整返回后停止计量，再检查结果和关闭重开。每种规模独立重复三轮，不能复用已经终态的任务制造低峰值。

各三轮完整 run_job 的 Python 分配峰值均不超过 24MiB，同类四倍规模的峰值中位增量不超过 4MiB，成功返回值也计量。基准逐轮输出 kind、backend、peak 字节、seconds、size、payload_bytes、round；seconds 用于解释执行代价，不另设未公开的耗时门槛。本目标是 Python 中间分配，不将 SQLite 原生内存误报为 Python tracemalloc 数据。

解析、结果写入、终态发布及事务提交都在计量内，不能挪到准备阶段、其他进程或测量结束之后。允许执行中借助 SQLite 内部处理，但不能把 Python 工作转交未计量的子进程。

## 联动验证和交付

保留所有原断言；新增小规模核账覆盖字段重排、独立预期、Unicode、联合和默认值、重复坐标、丢弃字段损坏先于读侧缺字段、读侧首错、合法前缀后晚失败或损坏、两个原钩子回滚及重试。负载的成功和失败结果还须重开核对。现有入口不替代交付者对新增暂存的读写异常和清理验证。

交付兼容优化代码、独立差分与必要异常验证、逐轮实测及简短表示交接说明。说明可信载荷/字段表示如何保持写分支身份与读侧访问次序，成功前缀由谁持有，晚失败如何撤销，成功返回的必要结果内存如何计入。不得用删除验证、删改参照或忽略异常达到资源目标。
