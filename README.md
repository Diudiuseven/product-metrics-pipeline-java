# Product Metrics Pipeline (Java)

产品指标的增量聚合与漏斗留存分析管道。原始行为事件持续到达，指标定义不断调整，查询方随时拿到可信的漏斗转化与留存结果；乱序、迟到、重复、事后更正都不能让结果失真。

## 特性

- **漏斗分析**：有序步骤 + 时间窗口，区分转化 / 跳步 / 超出窗口 / 缺失 / 未决；判定基准为用户已提交事件的最大行为时间，与查询时刻无关。
- **留存分析**：以每个用户首次锚点事件为准，按同期群给出可复现的留存。
- **乱序 / 迟到 / 重复 / 更正**：事件按行为时间重放；迟到更正已发布结果；重复幂等；删除墓碑。
- **增量 + 全量重算并存**：增量结果与全量重算逐条一致；重算期间新事件不丢失；快照原子发布，无新旧混合。
- **持续一致性比对**：每次提交后与同前缀全量重放逐条比对；不一致时门禁阻断新发布并给出定位到用户/分组的差异，修复性重算通过后自动恢复。
- **故障恢复**：检查点与快照同事务；中断的重算任务可区分、可清理、可续跑；门禁与比对报告持久化，重启后续跑修复。
- **维度切分 + 多指标并行**：高基数维度组；粘性归并规则保证增量与重算选出同一批分组；有界线程池，单指标变慢不阻塞其他部分；资源上界可配置。
- **定义版本化**：新版本不改变旧结果语义；旧版本无需迁移即可查询；非法/冲突定义加载时拒绝并定位到字段。
- **时间范围 + 分批查询**：keyset 分页 O(页大小)；语句超时不吐半份结果；不影响聚合。

## 快速开始

```bash
mvn spring-boot:run
```

服务启动于 `http://localhost:8080`。H2 数据文件位于 `./data/pipeline`。

### 摄入事件

```bash
curl -X POST http://localhost:8080/api/events -H 'Content-Type: application/json' -d '{
  "eventId": "evt-001",
  "userId": "u-123",
  "eventType": "page_view",
  "eventTime": "2026-10-01T10:00:00Z",
  "dims": {"country": "US", "platform": "ios"}
}'
```

### 注册指标定义

```bash
curl -X POST http://localhost:8080/api/metrics -H 'Content-Type: application/json' -d '{
  "id": "signup_funnel",
  "type": "FUNNEL",
  "funnel": {
    "steps": [
      {"id": "visit",  "eventType": "page_view"},
      {"id": "signup", "eventType": "signup"},
      {"id": "pay",    "eventType": "purchase"}
    ],
    "window": "PT1H",
    "windowFrom": "FIRST_STEP",
    "dimensions": ["country"]
  }
}'
```

### 查询漏斗

```bash
curl 'http://localhost:8080/api/metrics/signup_funnel/funnel?bucketFrom=2026-10-01&bucketTo=2026-10-01&pageSize=100'
```

## 文档

- [漏斗与留存语义](docs/funnel-retention-semantics.md)
- [一致性边界与故障恢复](docs/consistency-and-recovery.md)
- [配置项](docs/configuration.md)
- [REST API](docs/api.md)

## 测试

```bash
mvn test
```

覆盖场景：乱序到达、重复幂等、迟到更正、增量与重算并存、失败恢复、并发读写、非法定义、分页与时间范围、全量重算等价性、时间推移下判定可复现、无新事件时结果稳定、维度上界两条路径一致、比对失败门禁与修复、重启后恢复比对。
