# Product Metrics Pipeline (Java / Spring Boot)

面向持续到达的行为事件，提供**漏斗转化**与**留存**的增量聚合、可配置的版本化指标
定义、迟到/乱序/重复/事后更正处理，以及与全量重算严格对齐的可信查询结果。

## 能力概览

- 有序漏斗：多步骤按先后顺序且在总窗/单步窗内成立；跳步、缺失、超窗给出可区分判定。
- 留存：以每个用户首次锚点事件为 cohort 锚点，半开周期桶，结果可复现。
- 乱序/迟到/重复：`(eventId, revision)` 幂等去重，更高 revision 为更正；迟到更正
  增量生效，且与同批输入从头全量重算逐条一致（确定性纯函数 + NDJSON 行级 parity）。
- 增量与重算并存：重算在独立状态上构建，追平日志 tip 并校验后原子切换；期间新事件
  不丢失；查询任意时刻只看到某个已提交的自洽整体。
- 失败恢复：事件日志 fsync、快照两阶段原子提交、重算标记与检查点严格区分，重启后
  从最后提交位置继续。
- 维度切分与多指标并行：高基数字段确定性折叠为 `__OTHER__`，单指标变慢不阻塞其他，
  线程/内存/分组数均有可配置上界。
- 定义版本化：旧版本结果升级后无需迁移仍可查；非法/冲突定义加载期被拒绝并定位字段。
- 分批查询：NDJSON 顺序快照 + 行序号游标，不全量载入内存；超时/取消不出半页。

## 运行

```bash
mvn spring-boot:run
# 或
mvn package && java -jar target/product-metrics-pipeline-java-1.0-SNAPSHOT.jar
```

## 测试

```bash
mvn test
```

覆盖：乱序/重复/迟到更正、增量与全量重算逐条一致、失败恢复（空尾段/坏帧/未完成
重算标记）、并发读写、单指标隔离、高基数维度折叠、定义非法字段定位、HTTP 端到端等。

## 文档

- [docs/SEMANTICS.md](docs/SEMANTICS.md)：漏斗与留存的精确口径。
- [docs/CONSISTENCY.md](docs/CONSISTENCY.md)：一致性边界、重算协议、崩溃恢复。
- [docs/OPERATIONS.md](docs/OPERATIONS.md)：配置项、数据布局、HTTP API、容量规划。
