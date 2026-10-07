# 漏斗与留存语义

本文档定义产品指标管道中漏斗（Funnel）与留存（Retention）的计算语义。所有结果由事件日志确定性推导，与事件到达顺序无关。

## 事件模型

每条行为事件包含：`eventId`（幂等键）、`userId`、`eventType`、`eventTime`（行为发生时间）、`receiveTime`（管道接收时间）、`dimensions`（键值对）、`op`（PUT / CORRECT / DELETE）。

- 事件按 `eventTime` 排序后参与计算；乱序到达不影响结果。
- 同一 `eventId` 重复上报（payload 相同）不重复计数。
- 同一 `eventId` payload 变化 → 更正（CORRECT），旧记录保留但不参与计算。
- DELETE → 墓碑，该事件视为不存在。

## 漏斗（Funnel）

### 定义

```json
{
  "id": "signup_funnel",
  "type": "FUNNEL",
  "funnel": {
    "steps": [
      { "id": "visit",  "eventType": "page_view" },
      { "id": "signup", "eventType": "signup" },
      { "id": "pay",    "eventType": "purchase" }
    ],
    "window": "PT1H",
    "windowFrom": "FIRST_STEP",
    "dimensions": ["country", "platform"],
    "bucketGranularity": "DAY"
  }
}
```

- `steps`：有序步骤，至少 1 个。每个 step 可带 `dimFilters`（事件维度必须全部匹配该键值才算此步）。
- `window`：时间窗口（ISO-8601 duration）。多步漏斗必填，≤ 31 天。
- `windowFrom`：窗口起点。`FIRST_STEP`（默认，每步距首步不超过 window）或 `PREVIOUS_STEP`（每步距上一步不超过 window）。
- `dimensions`：切分维度。聚合按这些维度的取值分组；事件缺失某维度时记为 `__none__`。
- `bucketGranularity`：时间桶粒度，`DAY`（默认）或 `HOUR`。

### 判定（Verdict）

每个进入漏斗的用户恰好落入一个判定，互斥。优先级从高到低：

| 判定 | 含义 |
|------|------|
| `CONVERTED` | 所有步骤按顺序且在窗口内成立 |
| `SKIPPED_STEP` | 跳步：出现了靠后的步骤，但下一步骤未出现（如做了 step1 直接做 step3） |
| `OUT_OF_WINDOW` | 步骤顺序对，但相邻（或距首步）间隔超出窗口 |
| `MISSING` | 已进入，判定基准线已过窗口但未完成 |
| `PENDING` | 判定基准线仍在窗口内，尚未判定（结果未决） |
| `ENTERED` | 仅到达 step1（兜底） |

判定规则：

1. 用户首次出现 step1 事件 → 进入漏斗，开始一次 attempt。
2. attempt 开放期间：
   - 遇到 step1 事件 → 重新开始一次 attempt（旧 attempt 若已过窗口记 MISSING，否则视为被取代）。
   - 遇到**下一步骤**事件：在窗口内 → 前进一步；到达最后一步 → `CONVERTED`。超出窗口 → `OUT_OF_WINDOW`。
   - 遇到**更靠后**的步骤（跳过了下一步）→ `SKIPPED_STEP`。
   - 遇到不匹配任何步骤的事件 → 忽略。
3. 事件流结束时仍有开放 attempt：判定基准线（见下）已过窗口 → `MISSING`，否则 → `PENDING`。

最终判定取所有 attempt 中优先级最高者；步骤到达位掩码取各 attempt 的并集（用于分步转化人数）。

### 判定的时间基准（Decision Horizon）

"窗口是否已过期"**不看墙钟**，而是看**判定基准线**：该用户所有已提交事件中最大的 `eventTime`。

- 判定是已提交输入的纯函数：同一批已提交事件，无论在哪一刻查询、由增量路径还是全量重算路径计算，同一用户的判定唯一且相同。
- 没有新事件到达时，基准线不动，已提交的判定**不会自行漂移**（`PENDING` 不会因为墙钟走过窗口而自动变成 `MISSING`）。
- 只有当该用户**有新事件提交**、其基准线被推进到窗口之外时，开放中的 attempt 才判定为 `MISSING`。这正是一次"新输入到达"，允许判定更新。
- 迟到事件（`eventTime` 较早）不会推进基准线，因此不会把仍在窗口内的 attempt 误判为过期。

### 聚合

- 时间桶 = 首次 step1 事件的时间桶（`bucketGranularity`）。
- 维度组 = 首次 step1 事件的维度取值。
- `entered`：到达 step1 的去重用户数。
- `converted`：判定为 `CONVERTED` 的用户数。
- `stepCounts[k]`：到达第 k+1 步的去重用户数（按 attempt 并集）。
- 比率：`conversionRate = converted / entered`；`stepRate[k] = stepCounts[k] / entered`。

## 留存（Retention）

### 定义

```json
{
  "id": "app_retention",
  "type": "RETENTION",
  "retention": {
    "anchorEventType": "app_open",
    "returnEventType": "app_open",
    "periods": ["P1D", "P7D", "P30D"],
    "returnWindow": "P1D",
    "cohortGranularity": "DAY",
    "dimensions": ["country"]
  }
}
```

- `anchorEventType`：锚点事件。
- `returnEventType`：回访事件。
- `periods`：正时长列表，严格递增。
- `returnWindow`：回访判定窗口，默认与 `cohortGranularity` 相同（DAY → P1D）。
- `cohortGranularity`：同期群粒度，`DAY`（默认）或 `HOUR`。

### 语义

- **锚点**：用户**首次**出现 anchor 事件的时间（与事件到达顺序无关，由 `eventTime` 最小者决定）。
- **同期群**：锚点时间所在的时间桶。
- **第 i 期留存**：在 `[anchor + periods[i], anchor + periods[i] + returnWindow)` 区间内出现至少一次 return 事件。
- `cohortSize`：该同期群的锚点用户数。
- `retained[i]`：第 i 期回访用户数。
- `retentionRate[i] = retained[i] / cohortSize`。

结果可复现：给定同一批事件，锚点与留存判定唯一确定。

## 维度切分

- 维度取值事先未知，基数可能很高。聚合按实际出现的维度组建组，不预创建。
- 新增维度或新增指标不改变既有指标的结果（结果按 `metric_id + version` 隔离）。
- 资源上界见 `docs/configuration.md`。

### 维度上界与溢出归并

维度组数达到 `pipeline.max-dim-combinations` 上界时，超出的维度组归并到伪组 `__overflow__`。选择规则是**黏滞（sticky）且确定性**的：

1. 已经作为明细组提交的维度组**永远保留**——重算不会把已经稳定的组改写进溢出。
2. 剩余空位由新出现的维度组按字典序依次填充，与事件处理顺序、批切分方式无关。
3. 增量路径与全量重算路径使用同一份"已提交明细组"集合与同一条规则，因此对同一批已提交输入，两条路径选出的明细组/溢出组完全相同；重算前后已存在的组不会被改写。
4. `__overflow__` 本身不占用明细名额。
5. 一个组的最后一个用户离开后，该空组被移除（全量重算从不产生空组），其名额可被后续新维度组复用——两条路径同样一致。
