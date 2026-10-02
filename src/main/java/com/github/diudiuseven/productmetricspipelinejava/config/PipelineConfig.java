package com.github.diudiuseven.productmetricspipelinejava.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

/**
 * Bounded worker pool for metric builders. A slow metric only occupies its own
 * worker; other metrics keep making progress.
 */
@Configuration
public class PipelineConfig {

    @Bean("builderExecutor")
    public Executor builderExecutor(PipelineProperties props) {
        ThreadPoolTaskExecutor exec = new ThreadPoolTaskExecutor();
        exec.setCorePoolSize(props.getThreadPoolSize());
        exec.setMaxPoolSize(props.getThreadPoolSize());
        exec.setQueueCapacity(256);
        exec.setThreadNamePrefix("pipeline-builder-");
        exec.setWaitForTasksToCompleteOnShutdown(true);
        exec.setAwaitTerminationSeconds(30);
        // Backpressure: ingestion never blocks, but the scheduler skips a tick
        // when all workers are busy instead of growing queues unboundedly.
        exec.setRejectedExecutionHandler((r, e) -> { /* tick skipped; retried next interval */ });
        exec.initialize();
        return exec;
    }
}
