# Product Metrics Pipeline (Java)

产品指标的增量聚合与漏斗留存分析管道。原始行为事件持续到达，指标定义不断调整，查询方随时拿到可信的漏斗转化与留存结果；乱序、迟到、重复、事后更正都不能让结果失真。

## 特性

- **漏斗分析**：有序步骤 + 时间窗口，区分转化 / 跳步 / 超出窗口 / 缺失 / 未决。
- **留存分析**：以每个用户首次锚点事件为准，按同期群给出可复现的留存。
- **乱序 / 迟到 / 重复 / 更正**：事件按行为时间重放；迟到更正已发布结果；重复幂等；删除墓碑。
- **增量 + 全量重算并存**：增量结果与全量重算逐条一致；重算期间新事件不丢失；快照原子发布，无新旧混合。
- **故障恢复**：检查点与快照同事务；中断的重算任务可区分、可清理、可续跑。
- **维度切分 + 多指标并行**：高基数维度组；有界线程池，单指标变慢不阻塞其他部分；资源上界可配置。
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

覆盖场景：乱序到达、重复幂等、迟到更正、增量与重算并存、失败恢复、并发读写、非法定义、分页与时间范围、全量重算等价性。
