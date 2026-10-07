# REST API

## 事件摄入

### POST /api/events

摄入单条事件。

```json
{
  "eventId": "evt-001",
  "userId": "u-123",
  "eventType": "page_view",
  "eventTime": "2026-10-01T10:00:00Z",
  "dims": { "country": "US", "platform": "ios" },
  "op": "PUT"
}
```

- `eventId` 必填，幂等键。重复上报（相同 eventId + payload）返回已有 `logSeq`，不重复计数。
- `eventTime` 必填（ISO-8601）。
- `op`：`PUT`（默认）/ `CORRECT`（更正）/ `DELETE`（删除）。
- 响应：`{ logSeq, duplicate, corrected, deleted }`。

### POST /api/events/batch

批量摄入（原子：全部成功或全部失败）。请求体为事件数组。

## 指标定义

### GET /api/metrics

列出所有指标定义（所有版本）。

### GET /api/metrics/{id}?version={v}

获取指标定义。`version` 省略时返回最新版本。

### POST /api/metrics

注册新定义。请求体为定义 JSON。校验失败返回 HTTP 400，错误体：

```json
{
  "error": "INVALID_DEFINITION",
  "errors": [
    { "field": "funnel.steps[1].eventType", "code": "REQUIRED",
      "message": "step eventType is required", "rejectedValue": "" }
  ]
}
```

版本化：同 id 重复注册 → 版本号 +1；指定已存在版本号 → HTTP 409 CONFLICT。旧版本结果不变。

## 漏斗查询

### GET /api/metrics/{id}/funnel

参数：`version`、`bucketFrom`、`bucketTo`（含）、`dimFilter`（JSON，如 `{"country":"US"}`）、`pageSize`、`pageToken`。

返回：

```json
{
  "rows": [
    {
      "bucket": "2026-10-01",
      "dims": "country=US",
      "entered": 100,
      "converted": 25,
      "skipped": 5,
      "missing": 10,
      "outOfWindow": 3,
      "pending": 57,
      "stepCounts": [100, 60, 25],
      "stepRates": [1.0, 0.6, 0.25],
      "conversionRate": 0.25
    }
  ],
  "nextPageToken": "eyJ...",
  "commitId": 42,
  "logSeqTo": 1337,
  "totalOnPage": 1
}
```

- 无 `dimFilter` 时返回每个维度组一行；有 `dimFilter` 时返回匹配的维度组。
- `pageToken` 为不透明游标；分页大小 O(页大小)。

## 留存查询

### GET /api/metrics/{id}/retention

参数同上（`cohortFrom` / `cohortTo`）。

返回行：

```json
{
  "cohort": "2026-10-01",
  "dims": "__all__",
  "cohortSize": 100,
  "retained": [40, 25, 10],
  "retentionRates": [0.4, 0.25, 0.1]
}
```

## 管理

### POST /api/admin/recompute/{id}?version={v}

触发全量重算（异步）。重算期间新事件不丢失；发布原子替换。

### GET /api/admin/commits/{id}?version={v}

列出最近提交记录（含 `logSeqFrom/To`、`eventCount`、`trigger`、`status`），用于解释结果来源。

### GET /api/admin/verification/{id}?version={v}&limit={n}

列出最近的一致性比对报告。`status` 为可区分的失败原因：`VERIFIED` / `MISMATCH` / `SKIPPED_BOUNDS` / `HORIZON_UNDETERMINED`；`MISMATCH` 时 `diffs`（JSON）逐条给出具体用户（`user <id>`）与具体分组（`group <bucket␀dims>`）的 `expected/actual` 差异。

### POST /api/admin/verify/{id}?version={v}

对已提交快照触发一次即时一致性比对（异步），结果写入比对报告。
