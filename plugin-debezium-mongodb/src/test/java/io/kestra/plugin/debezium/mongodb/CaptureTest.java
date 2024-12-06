package io.kestra.plugin.debezium.mongodb;

import com.google.common.base.Charsets;
import io.kestra.core.models.property.Property;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
import io.kestra.core.junit.annotations.KestraTest;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import java.io.*;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
@Disabled("Until there will be automatic way to execute mongo.js scripts")
class CaptureTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        Capture task = Capture.builder()
            .id(IdUtils.create())
            .type(Capture.class.getName())
            .snapshotMode(Property.of(MongodbInterface.SnapshotMode.INITIAL))
            .connectionString(Property.of("mongodb://mongo_user:mongo_passwd@127.0.0.1:27017/?replicaSet=rs0"))
            .maxRecords(Property.of(20))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        AbstractDebeziumTask.Output runOutput = task.run(runContext);

        assertThat(runOutput.getSize(), is(20));

        List<Map<String, Object>> employee = new ArrayList<>();
        FileSerde.reader(new BufferedReader(new InputStreamReader(storageInterface.get(null, null, runOutput.getUris().get("second.employeeTerritory")))), r -> employee.add((Map<String, Object>) r));

        List<Map<String, Object>> types = new ArrayList<>();
        FileSerde.reader(new BufferedReader(new InputStreamReader(storageInterface.get(null, null, runOutput.getUris().get("kestra.mongo_types")))), r -> types.add((Map<String, Object>) r));

        IOUtils.toString(storageInterface.get(null, null, runOutput.getUris().get("kestra.mongo_types")), Charsets.UTF_8);
        assertThat(employee.size(), is(14));
        assertThat(employee.stream().filter(o -> !((Boolean) o.get("deleted"))).count(), is(7L));

        FileSerde.reader(new BufferedReader(new InputStreamReader(storageInterface.get(null, null, runOutput.getUris().get("second.employeeTerritory")))), r -> employee.add((Map<String, Object>) r));
        assertThat(types.size(), is(6));
        assertThat(types.stream().filter(o -> !((Boolean) o.get("deleted"))).count(), is(4L));

        // rerun state will prevent new records
        runOutput = task.run(runContext);
        assertThat(runOutput.getSize(), is(0));
    }
}