package com.github.diudiuseven.productmetricspipelinejava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.diudiuseven.productmetricspipelinejava.model.DefinitionException;
import com.github.diudiuseven.productmetricspipelinejava.model.FunnelStep;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricDefinition;
import com.github.diudiuseven.productmetricspipelinejava.model.MetricType;
import com.github.diudiuseven.productmetricspipelinejava.store.FileDefinitionsRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefinitionsRepositoryTest {

    @TempDir
    Path tmp;

    private MetricDefinition funnel(String id, int version, long window) {
        return MetricDefinition.builder().id(id).version(version).type(MetricType.FUNNEL)
                .steps(List.of(new FunnelStep(0, "view"), new FunnelStep(1, "buy")))
                .totalWindowMillis(window).stepWindowMillis(window).build();
    }

    @Test
    void sameVersionDifferentPayloadIsConflictWithFieldLocation() {
        FileDefinitionsRepository repo = new FileDefinitionsRepository(tmp.toString(), new ObjectMapper());
        repo.register(funnel("f", 1, 100));
        DefinitionException ex = assertThrows(DefinitionException.class,
                () -> repo.register(funnel("f", 1, 200)));
        assertTrue(ex.problems().stream().anyMatch(p -> p.field().equals("version")),
                "conflict must point at the version field");
    }

    @Test
    void sameVersionSamePayloadIsIdempotent() {
        FileDefinitionsRepository repo = new FileDefinitionsRepository(tmp.toString(), new ObjectMapper());
        MetricDefinition d = funnel("f", 1, 100);
        repo.register(d);
        repo.register(funnel("f", 1, 100)); // identical re-registration
        assertEquals(1, repo.versions("f").size());
    }

    @Test
    void newVersionDoesNotChangeOldVersionAndActiveMovesForward() {
        FileDefinitionsRepository repo = new FileDefinitionsRepository(tmp.toString(), new ObjectMapper());
        repo.register(funnel("f", 1, 100));
        repo.register(funnel("f", 2, 200));
        assertEquals(100, repo.get("f", 1).totalWindowMillis());
        assertEquals(200, repo.get("f", 2).totalWindowMillis());
        assertEquals(2, repo.active("f").version());
    }

    @Test
    void oldVersionsSurviveRestartWithoutMigration() {
        new FileDefinitionsRepository(tmp.toString(), new ObjectMapper()).register(funnel("f", 1, 100));
        FileDefinitionsRepository reopened = new FileDefinitionsRepository(tmp.toString(), new ObjectMapper());
        reopened.loadAll();
        assertEquals(1, reopened.active("f").version());
        assertEquals(100, reopened.get("f", 1).totalWindowMillis());
    }
}
