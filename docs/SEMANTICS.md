# 漏斗与留存语义

本文件定义产品指标管线对**漏斗转化**与**留存**的精确口径。所有判定都是存储事件的
确定性纯函数，因此同一份事件日志从头全量重算与增量推进的结果逐条一致。

## 0. 事件的时间与身份

- `eventTime`：行为实际发生时间（可乱序、迟到）。
- `ingestTime`：管线持久化该事件的时间。
- `eventId`：业务幂等身份。同一 `eventId`：
  - `revision` 相等或更低 → 视为**重复上报**，直接拒绝，不重复计数；
  - `revision` 更高 → 视为**事后更正**，旧事实被新事实整体替换（即使改了
    用户、事件名、时间或维度），聚合结果随之更正。
- 全局全序：管线为每个持久化事件分配无空隙、单调递增的 `seq`。事件之间的先后
  统一按 **`(eventTime, seq)`** 判定。

## 1. 漏斗（Funnel）

定义包含：有序步骤 `steps`、总时间窗 `totalWindowMillis`、
单步时间窗 `stepWindowMillis`、维度键 `dimensionKeys`。

### 1.1 一次旅程（Journey）

对某用户：

1. 仅考虑事件名属于步骤集合的事件，按 `(eventTime, seq)` 排序；
2. 每个落在查询区间 `[from, to]` 内的**第 0 步事件**都是一个候选锚点；
3. 从锚点出发，第 k 步取**严格晚于**当前步骤（eventTime 更大；eventTime 相等时
   seq 更大）且事件名匹配的最早事件，并同时满足：
   - `t <= anchorTime + totalWindowMillis`（总窗）
   - `t <= prevStepTime + stepWindowMillis`（单步窗）
   - `t <= to`（查询上界）
4. 在所有候选锚点走出的旅程中，**到达步骤最远者胜出**；并列时取锚点最早
   `(eventTime, seq)` 的旅程。这一决断保证结果可复现。

### 1.2 三类可区分判定

| 状态 | 含义 |
| --- | --- |
| `CONVERTED` | 所有步骤按顺序、在总窗与各单步窗内成立 |
| `DROPPED` | 进入了漏斗（存在合格锚点），但某后续步骤未按时出现；结果携带最后到达步骤 |
| `NOT_STARTED` | 查询区间内没有合格的第 0 步锚点 |

因此：跳步、缺失步骤、超出总窗、超出单步窗、乱序插入，都不会被记为转化，
且丢弃位置可区分（`reachedStep` 与每步到达计数）。

### 1.3 维度归属

分组维度取自**被选中旅程的锚点事件**（第 0 步）上的维度值。锚点因更正而变化时，
分组随新事实移动。

## 2. 留存（Retention）

定义包含：锚点事件 `anchorEvent`、留存事件 `retainedEvent`、
留存周期 `retentionPeriodsMillis`（严格递增）、cohort 桶 `cohortBucketMillis`。

- **锚点**：每个用户**第一个**锚点事件（`(eventTime, seq)` 最早）。迟到更早的
  锚点会把该用户整体迁移到更早 cohort，结果随之更正。
- **cohort 起点**：`floor(anchorTime / cohortBucketMillis) * cohortBucketMillis`。
- 查询区间对锚点生效：`anchorTime ∈ [from, to)` 才计入本次查询的 cohort。
- **周期桶（半开区间，可区分、非累计）**：第 i 个周期命中当且仅当存在留存事件
  - 下界：`anchorTime + (i==0 ? 0 : periods[i-1])`
  - 上界：`anchorTime + periods[i]`
  - 满足 `下界 <= t < 上界`。恰好在边界上的事件归入更晚的桶。
- 同一周期内多次留存事件只表示“命中”，不重复计数。

## 3. 维度切分与基数上界

维度取值事先未知。每个指标有 `maxDimensionGroups` 上界：

- 读侧按维度元组字典序保留最小的 K 个真实分组；
- 超出部分确定性地折叠进 `__OTHER__` 组；
- `__OTHER__` 行携带被折叠用户数，核心进入/转化/留存计数仍然精确，只是维度归属被折叠；
- 新增维度键或新增指标不会改变任何既有指标版本的结果（见版本化语义）。

## 4. 指标定义版本化

- 标识为 `(metricId, version)`；版本号单调递增。
- 已发布版本**不可变**：同版本不同内容的再次加载在加载期被拒绝，并定位到字段。
- 新版本不改变旧版本已产出结果的语义；旧版本结果持久保留、升级后无需迁移仍可查询
  （查询可显式带 `version`）。
- active 指针只向前移动。

非法定义在**加载时**拒绝，错误定位到具体字段，例如
`steps[1].order`、`totalWindowMillis`、`retentionPeriodsMillis[1]`。
