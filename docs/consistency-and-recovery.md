# 一致性边界与故障恢复

## 架构总览

```
事件摄入(API) ──► 事件日志(append-only, 持久化)
                        │
                        ▼
              增量构建器(每指标一个, 有界线程池)
              - 按 eventTime 重放受影响用户
              - 事务内发布快照 + 检查点
                        │
                        ▼
              快照表(当前已提交状态) ◄── 重算服务(全量, scratch 原子发布)
                        │
                        ▼
              查询服务(keyset 分页, 语句超时)
```

## 一致性模型

### 1. 事件日志是唯一事实来源

- 事件先持久化到 append-only 日志，再参与聚合。
- 聚合状态可随时从日志重算得到。

### 2. 增量结果与全量重算逐条一致

增量构建器不做近似：每批处理后，对受影响用户**从其全部事件历史**重放状态机，再以用户为单位对聚合做加减 diff。因此：

- 乱序事件：按 `eventTime` 排序重放，结果与顺序无关。
- 迟到事件：重放受影响用户的完整历史，已发布的聚合被更正。
- 重复事件：幂等，不重复计数。
- 更正/删除：以最新记录为准。

任意时刻，对任意指标版本，`增量结果 == 全量重算结果`（逐条比对相等）。

### 3. 快照原子发布（无新旧混合）

- 每次发布是一个数据库事务：写入聚合行 + 用户贡献行 + 提交记录 + 检查点，要么全部生效要么全部不生效。
- 查询只读取已提交数据（H2 MVCC 语句级读一致性），永远不会读到半份结果。
- 提交记录（`snapshot_commit`）记录 `log_seq_from / log_seq_to / event_count / trigger(INCREMENTAL|RECOMPUTE)`，可解释每版结果对应的输入范围。

### 4. 增量与重算并存

- 重算期间到达的新事件：正常写入事件日志，不丢失。
- 重算以启动时的最大 `log_seq` 为截止；发布后检查点推进到该截止。
- 截止之后的新事件由后续增量批次处理（同样是重放受影响用户），最终状态等价于对全部事件的全量重算。
- 重算通过 scratch 表构建，发布时在一个事务内用 scratch 替换当前状态；替换期间查询仍读旧快照，发布后读新快照。

### 5. 定义版本隔离

- 结果按 `(metric_id, version)` 键控。新版本不触碰旧版本的表行。
- 旧版本无需迁移即可继续查询。

## 故障恢复

### 检查点续跑

- 每个 `(metric_id, version)` 的消费位置（`log_seq`）与最近提交 ID 持久化在 `builder_checkpoint`，与快照更新在**同一事务**。
- 进程重启后，构建器从检查点继续：`log_seq` 之前的事件不再处理，之后的事件正常处理。不重复计入，不跳过事件。

### 中断的重算

- 重算任务记录在 `recompute_task`（状态 `RUNNING / DONE / INTERRUPTED / FAILED`），scratch 行按 `task_id` 隔离。
- 重启时，所有遗留 `RUNNING` 任务标记为 `INTERRUPTED` 并删除其 scratch；已提交的快照不受影响。
- 已提交结果（`snapshot_commit` 状态 `COMMITTED`）与未完成任务（`recompute_task` 状态 `RUNNING/INTERRUPTED`）在表结构上可区分。

### 幂等摄入

- 重复上报同一事件（相同 eventId + payload）返回已有 `log_seq`，不产生新行。
- 并发重复摄入由数据库唯一约束 `(event_id, payload_hash)` 兜底；捕获冲突后按幂等处理。

## 查询一致性

- 查询读取最新已提交快照（可通过 `version` 指定定义版本）。
- keyset 分页：`WHERE (bucket, dims) > (?, ?) ORDER BY bucket, dims LIMIT n`，成本 O(页大小)，不扫描全量。
- 语句超时：`pipeline.query-timeout`（默认 5s）。超时抛出 `QueryTimeoutException`（HTTP 504），不返回任何行。
- 查询取消/超时不影响正在进行的聚合（只读事务 + MVCC）。

### 查询耗时量级上界

- 单页查询：O(pageSize) 次索引定位，无全表扫描。本地 SSD 上 pageSize ≤ 200 时典型 P99 < 100ms。
- 全量重算：O(事件总数)，但不在查询路径上；查询永远读快照，不重算。
- 内存：分页查询不把全部结果载入内存（每次只取 pageSize+1 行）。

## 有界资源

| 配置 | 默认 | 作用 |
|------|------|------|
| `pipeline.max-batch-events` | 5000 | 单批最多处理的事件数 |
| `pipeline.max-affected-users` | 2000 | 单批最多重放的用户数，超出触发全量重算 |
| `pipeline.max-events-per-user` | 10000 | 单用户事件数安全熔断，超出触发全量重算 |
| `pipeline.max-dim-combinations` | 50000 | 维度组数上界（超出部分计入 `__overflow__`，见提交摘要） |
| `pipeline.recompute-max-events` | 2000000 | 重算事件数告警阈值（超出仍重算） |
| `pipeline.thread-pool-size` | 4 | 指标构建器并行线程数 |
| `pipeline.query-timeout` | 5s | 查询硬超时 |
| `pipeline.default-page-size` | 100 | 默认分页大小 |
| `pipeline.max-page-size` | 1000 | 分页大小上界 |

单个指标变慢只占用其自身的构建线程，不阻塞其他指标。
