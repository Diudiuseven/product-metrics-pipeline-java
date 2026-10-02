# 配置项、HTTP API 与运维

## 1. 数据目录布局

```
<dataDir>/
  log/000000.seg ...            # 仅追加事件日志（64MB 滚动）
  definitions/<id>/v<n>.json    # 不可变定义版本
  definitions/<id>/active.ptr   # 当前生效版本
  snapshots/<id>/v<n>/*.ndjson  # 不可变 NDJSON 快照
  snapshots/<id>/v<n>/current.ptr
  recompute/<id>/v<n>.marker    # 仅未完成重算存在
```

## 2. 配置项（application.properties，前缀 pipeline.）

| 键 | 默认值 | 含义 / 上界 |
| --- | --- | --- |
| `data-dir` | `${java.io.tmpdir}/product-metrics-pipeline` | 日志、快照、定义、重算标记的根目录 |
| `drain-batch-size` | 500 | 每个聚合器每次从日志拉取的事件数（单批原子提交） |
| `worker-threads` | 4 | 指标并行排水的固定线程池大小（并行度/线程上界） |
| `max-events-per-metric` | 5_000_000 | 每指标在内存保留事件数的硬上界，超限触发背压 |
| `max-dimension-groups` | 10_000 | 维度分组基数上界，超出折叠为 `__OTHER__` |
| `max-out-of-order-horizon-millis` | 604_800_000 (7 天) | 摄入时允许的迟到时间跨度；更老的事件被拒绝 |
| `query-timeout-millis` | 2_000 | 单次查询墙上时间预算，超时返回 504 且不出半页 |
| `query-page-size` | 1_000 | 默认/参考单页行数（请求 pageSize 不得超过其 10 倍） |
| `drain-interval-millis` | 20 | 无新事件时监督循环的空闲睡眠间隔 |
| `cohort-bucket-millis`（定义内） | 86_400_000 (1 天) | 留存 cohort 桶宽，每个留存指标可单独配置 |

### 容量说明

- 内存主要随“每指标保留的相关事件数”和“唯一 eventId 去重索引”增长。
- 生产部署应将 `max-events-per-metric` 与 JVM 堆、磁盘按事件平均大小联合规划；
  事件日志与快照是磁盘上的顺序文件，查询不要求把全量读入堆内存。
- 快照保留最新与上一版（供可能的在读查询），更旧快照自动清理。

## 3. HTTP API

### 3.1 摄入事件

`POST /api/events`
```json
{"events":[
  {"eventId":"e1","userId":"u1","name":"view","eventTime":1700000000000,
   "dimensions":{"plan":"free"},"revision":0}
]}
```
响应：`{"lastSeq":..,"appended":N,"duplicateIds":[...]}`。
重复（同 id 且 revision 不更高）与超出迟到地平线的 id 出现在 `duplicateIds`，
不被计数。摄入成功即已 fsync 持久化。

### 3.2 注册/查询定义

- `POST /api/definitions`：校验并持久化一个版本。非法或冲突返回 400，
  body 中 `fields` 把原因定位到具体字段。
- `GET /api/definitions/{metricId}`：列出全部历史版本（含旧版本，无需迁移）。

漏斗定义示例：
```json
{"id":"shop","version":1,"type":"FUNNEL",
 "steps":[{"order":0,"eventName":"view"},{"order":1,"eventName":"buy"}],
 "totalWindowMillis":3600000,"stepWindowMillis":3600000,
 "dimensionKeys":["plan"],"maxDimensionGroups":10000}
```
留存定义示例：
```json
{"id":"ret","version":1,"type":"RETENTION",
 "anchorEvent":"signup","retainedEvent":"active",
 "retentionPeriodsMillis":[86400000,172800000,259200000],
 "cohortBucketMillis":86400000,"dimensionKeys":["plan"]}
```

### 3.3 分批查询（游标分页 + 时间范围）

- `GET /api/query/funnel/users?metricId=&version=&from=&to=&cursor=&pageSize=`
- `GET /api/query/funnel/report?...`
- `GET /api/query/retention?...`

返回：
```json
{"rows":[...],"nextFrom":"<opaque-cursor>", "complete":false}
```
- `from`/`to` 为 eventTime 毫秒；漏斗闭区间 `[from,to]`，留存锚点半开 `[from,to)`。
- 首页不带 cursor；用 `nextFrom` 取后续页，`complete=true` 表示结束。
- 超时/取消返回 504，且不会吐出半份结果。

### 3.4 运维

- `GET /api/status/lag`：每个 (指标,版本) 的 `committedSeq/latestSeq/lag`。
- `POST /api/admin/recompute/{metricId}?version=`：全量重算并在校验后原子切换；
  失败保留旧结果。

## 4. 典型运维场景

- **迟到更正**：直接摄入同 id 更高 revision 事件即可，增量状态自动更正；若需要
  显式校验，可触发一次 recompute，parity 不一致会报错而不是切换。
- **扩新指标/新版本**：注册后自动建聚合器并从日志头追平，不影响既有指标版本。
- **进程重启**：无需人工干预，从最后检查点继续；启动时未完成重算被登记为 aborted。
- **某指标维度暴涨**：超过 `maxDimensionGroups` 的分组确定性折叠进 `__OTHER__`，
  核心计数不丢，可在结果中看到折叠用户数。
