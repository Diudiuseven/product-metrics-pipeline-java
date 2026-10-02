-- Product metrics pipeline schema (H2 2.x)
-- All state is transactional: snapshot commits are atomic, readers only ever
-- see committed rows (MVCC). See docs/consistency-and-recovery.md.

-- ---------------- event log (source of truth, append-only) ----------------
CREATE TABLE IF NOT EXISTS event_log (
    log_seq      BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id     VARCHAR(128) NOT NULL UNIQUE,
    user_id      VARCHAR(128) NOT NULL,
    event_type   VARCHAR(128) NOT NULL,
    event_time   TIMESTAMP NOT NULL,
    receive_time TIMESTAMP NOT NULL,
    dims         CLOB,
    op           VARCHAR(16) NOT NULL,           -- PUT / CORRECT / DELETE
    payload_hash VARCHAR(64) NOT NULL,
    prev_seq     BIGINT
);
CREATE INDEX IF NOT EXISTS idx_event_user_time ON event_log(user_id, event_time);
CREATE INDEX IF NOT EXISTS idx_event_time ON event_log(event_time);
CREATE INDEX IF NOT EXISTS idx_event_type_time ON event_log(event_type, event_time);

-- ---------------- metric definitions (versioned) ----------------
CREATE TABLE IF NOT EXISTS metric_def (
    id         VARCHAR(64) NOT NULL,
    version    INT NOT NULL,
    spec       CLOB NOT NULL,
    status     VARCHAR(16) NOT NULL,            -- ACTIVE / ARCHIVED
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (id, version)
);

-- ---------------- snapshot commits (explainability + recovery) ----------------
CREATE TABLE IF NOT EXISTS snapshot_commit (
    commit_id     BIGINT AUTO_INCREMENT PRIMARY KEY,
    metric_id     VARCHAR(64) NOT NULL,
    version       INT NOT NULL,
    log_seq_from  BIGINT NOT NULL,
    log_seq_to    BIGINT NOT NULL,
    event_count   BIGINT NOT NULL,
    affected_users BIGINT NOT NULL DEFAULT 0,
    trigger       VARCHAR(32) NOT NULL,         -- INCREMENTAL / RECOMPUTE
    status        VARCHAR(16) NOT NULL,         -- COMMITTED
    summary       CLOB,
    created_at    TIMESTAMP NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_commit_metric ON snapshot_commit(metric_id, version, commit_id);

-- ---------------- current funnel aggregates (latest committed snapshot) ----------------
CREATE TABLE IF NOT EXISTS funnel_agg (
    metric_id     VARCHAR(64) NOT NULL,
    version       INT NOT NULL,
    bucket        VARCHAR(32) NOT NULL,
    dims          VARCHAR(512) NOT NULL,
    entered       BIGINT NOT NULL DEFAULT 0,
    converted     BIGINT NOT NULL DEFAULT 0,
    skipped       BIGINT NOT NULL DEFAULT 0,
    missing       BIGINT NOT NULL DEFAULT 0,
    out_of_window BIGINT NOT NULL DEFAULT 0,
    pending       BIGINT NOT NULL DEFAULT 0,
    step_counts   CLOB NOT NULL,
    PRIMARY KEY (metric_id, version, bucket, dims)
);

-- ---------------- current per-user funnel contribution (incremental diff base) ----------------
CREATE TABLE IF NOT EXISTS funnel_user (
    metric_id  VARCHAR(64) NOT NULL,
    version    INT NOT NULL,
    user_id    VARCHAR(128) NOT NULL,
    bucket     VARCHAR(32) NOT NULL,
    dims       VARCHAR(512) NOT NULL,
    step_mask  INT NOT NULL,
    verdict    VARCHAR(16) NOT NULL,
    PRIMARY KEY (metric_id, version, user_id)
);
CREATE INDEX IF NOT EXISTS idx_funnel_user_bucket ON funnel_user(metric_id, version, bucket);

-- ---------------- current retention aggregates ----------------
CREATE TABLE IF NOT EXISTS retention_agg (
    metric_id   VARCHAR(64) NOT NULL,
    version     INT NOT NULL,
    cohort      VARCHAR(32) NOT NULL,
    dims        VARCHAR(512) NOT NULL,
    cohort_size BIGINT NOT NULL DEFAULT 0,
    retained    CLOB NOT NULL,
    PRIMARY KEY (metric_id, version, cohort, dims)
);

-- ---------------- current per-user retention contribution ----------------
CREATE TABLE IF NOT EXISTS retention_user (
    metric_id VARCHAR(64) NOT NULL,
    version   INT NOT NULL,
    user_id   VARCHAR(128) NOT NULL,
    cohort    VARCHAR(32) NOT NULL,
    dims      VARCHAR(512) NOT NULL,
    mask      INT NOT NULL,
    PRIMARY KEY (metric_id, version, user_id)
);

-- ---------------- builder checkpoints (resume position) ----------------
CREATE TABLE IF NOT EXISTS builder_checkpoint (
    metric_id      VARCHAR(64) NOT NULL,
    version        INT NOT NULL,
    log_seq        BIGINT NOT NULL,
    last_commit_id BIGINT NOT NULL,
    updated_at     TIMESTAMP NOT NULL,
    PRIMARY KEY (metric_id, version)
);

-- ---------------- recompute tasks (scratch builds; recoverable) ----------------
CREATE TABLE IF NOT EXISTS recompute_task (
    task_id      BIGINT AUTO_INCREMENT PRIMARY KEY,
    metric_id    VARCHAR(64) NOT NULL,
    version      INT NOT NULL,
    log_seq_from BIGINT NOT NULL,
    log_seq_to   BIGINT NOT NULL,
    status       VARCHAR(16) NOT NULL,          -- RUNNING / DONE / INTERRUPTED / FAILED
    started_at   TIMESTAMP NOT NULL,
    finished_at  TIMESTAMP,
    note         VARCHAR(512)
);

-- ---------------- recompute scratch tables (dropped/published atomically) ----------------
CREATE TABLE IF NOT EXISTS funnel_agg_scratch (
    task_id BIGINT NOT NULL,
    bucket  VARCHAR(32) NOT NULL,
    dims    VARCHAR(512) NOT NULL,
    entered BIGINT NOT NULL DEFAULT 0,
    converted BIGINT NOT NULL DEFAULT 0,
    skipped BIGINT NOT NULL DEFAULT 0,
    missing BIGINT NOT NULL DEFAULT 0,
    out_of_window BIGINT NOT NULL DEFAULT 0,
    pending BIGINT NOT NULL DEFAULT 0,
    step_counts CLOB NOT NULL,
    PRIMARY KEY (task_id, bucket, dims)
);
CREATE TABLE IF NOT EXISTS funnel_user_scratch (
    task_id BIGINT NOT NULL,
    user_id VARCHAR(128) NOT NULL,
    bucket  VARCHAR(32) NOT NULL,
    dims    VARCHAR(512) NOT NULL,
    step_mask INT NOT NULL,
    verdict VARCHAR(16) NOT NULL,
    PRIMARY KEY (task_id, user_id)
);
CREATE TABLE IF NOT EXISTS retention_agg_scratch (
    task_id BIGINT NOT NULL,
    cohort  VARCHAR(32) NOT NULL,
    dims    VARCHAR(512) NOT NULL,
    cohort_size BIGINT NOT NULL DEFAULT 0,
    retained CLOB NOT NULL,
    PRIMARY KEY (task_id, cohort, dims)
);
CREATE TABLE IF NOT EXISTS retention_user_scratch (
    task_id BIGINT NOT NULL,
    user_id VARCHAR(128) NOT NULL,
    cohort  VARCHAR(32) NOT NULL,
    dims    VARCHAR(512) NOT NULL,
    mask    INT NOT NULL,
    PRIMARY KEY (task_id, user_id)
);
