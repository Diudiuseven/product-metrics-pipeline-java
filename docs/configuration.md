# 配置项

所有配置项前缀为 `pipeline`，在 `application.properties` 中设置。

| 键 | 默认值 | 说明 |
|----|--------|------|
| `pipeline.batch-interval` | `200ms` | 构建器轮询事件日志的间隔。支持 Duration（如 `200ms`、`1s`）或毫秒数。 |
| `pipeline.max-batch-events` | `5000` | 单个增量批次最多消费的事件数。 |
| `pipeline.max-affected-users` | `2000` | 单批最多重放的去重用户数；超出则升级为全量重算。 |
| `pipeline.max-events-per-user` | `10000` | 单用户事件数安全熔断；超出则升级为全量重算。 |
| `pipeline.max-dim-combinations` | `50000` | 单指标维度组数上界。 |
| `pipeline.recompute-max-events` | `2000000` | 全量重算事件数告警阈值（超出仍重算，仅告警）。 |
| `pipeline.thread-pool-size` | `4` | 指标构建器并行线程数（有界线程池，队列容量 256）。 |
| `pipeline.query-timeout` | `5s` | 查询硬超时；超时返回 HTTP 504 且不输出任何行。 |
| `pipeline.default-page-size` | `100` | 查询默认分页大小。 |
| `pipeline.max-page-size` | `1000` | 分页大小上界；超出被截断。 |
| `pipeline.metrics-dir` | `./metrics` | 指标定义 JSON 文件目录（启动时加载）。 |
| `pipeline.bucket-granularity` | `DAY` | 全局默认时间桶粒度（`HOUR` / `DAY`）；定义内可覆盖。 |

## 数据源

```properties
spring.datasource.url=jdbc:h2:file:./data/pipeline;AUTO_SERVER=TRUE;DB_CLOSE_DELAY=-1
spring.datasource.driver-class-name=org.h2.Driver
spring.sql.init.mode=always
spring.sql.init.schema-locations=classpath:schema.sql
```

- 默认使用文件型 H2（`./data/pipeline`），进程重启后数据保留。
- 测试使用内存型 H2（`jdbc:h2:mem:testdb`）。

## 指标定义文件

启动时从 classpath `metrics/*.json` 与 `pipeline.metrics-dir` 目录加载。每个文件一个定义（或定义数组）。定义被严格校验，非法定义在启动时快速失败（字段级错误）。

通过 API 动态注册的定义持久化到 `metric_def` 表，重启后保留。
