package com.github.diudiuseven.productmetricspipelinejava.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.diudiuseven.productmetricspipelinejava.engine.PipelineOrchestrator;
import com.github.diudiuseven.productmetricspipelinejava.query.QueryService;
import com.github.diudiuseven.productmetricspipelinejava.store.DefinitionsRepository;
import com.github.diudiuseven.productmetricspipelinejava.store.EventLog;
import com.github.diudiuseven.productmetricspipelinejava.store.FileDefinitionsRepository;
import com.github.diudiuseven.productmetricspipelinejava.store.FileEventLog;
import com.github.diudiuseven.productmetricspipelinejava.store.FileSnapshotStore;
import com.github.diudiuseven.productmetricspipelinejava.store.SnapshotStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(PipelineProperties.class)
public class PipelineConfiguration {

    @Bean
    public EventLog eventLog(PipelineProperties props) {
        return new FileEventLog(props.getDataDir());
    }

    @Bean
    public SnapshotStore snapshotStore(PipelineProperties props, ObjectMapper mapper) {
        return new FileSnapshotStore(props.getDataDir(), mapper);
    }

    @Bean
    public DefinitionsRepository definitionsRepository(PipelineProperties props, ObjectMapper mapper) {
        return new FileDefinitionsRepository(props.getDataDir(), mapper);
    }

    @Bean(initMethod = "start")
    public PipelineOrchestrator orchestrator(EventLog eventLog,
                                             SnapshotStore snapshotStore,
                                             DefinitionsRepository definitions,
                                             PipelineProperties props) {
        return new PipelineOrchestrator(eventLog, snapshotStore, definitions, props);
    }

    @Bean
    public QueryService queryService(PipelineOrchestrator orchestrator) {
        return new QueryService(orchestrator);
    }
}
