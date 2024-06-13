package io.kestra.plugin.debezium.mongodb;

import com.google.common.base.Charsets;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.debezium.AbstractDebeziumTask;
import io.kestra.plugin.debezium.AbstractDebeziumTest;
import io.kestra.plugin.debezium.mongodb.Capture;
import io.kestra.plugin.debezium.mongodb.MongodbInterface;
import io.kestra.core.junit.annotations.KestraTest;
import org.apache.commons.io.IOUtils;
import org.h2.tools.RunScript;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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
            .snapshotMode(MongodbInterface.SnapshotMode.INITIAL)
            .connectionString("mongodb://mongo_user:mongo_passwd@127.0.0.1:27017/?replicaSet=rs0")
            .maxRecords(20)
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        AbstractDebeziumTask.Output runOutput = task.run(runContext);

        assertThat(runOutput.getSize(), is(20));

        List<Map<String, Object>> employee = new ArrayList<>();
        FileSerde.reader(new BufferedReader(new InputStreamReader(storageInterface.get(null, runOutput.getUris().get("second.employeeTerritory")))), r -> employee.add((Map<String, Object>) r));

        List<Map<String, Object>> types = new ArrayList<>();
        FileSerde.reader(new BufferedReader(new InputStreamReader(storageInterface.get(null, runOutput.getUris().get("kestra.mongo_types")))), r -> types.add((Map<String, Object>) r));

        IOUtils.toString(storageInterface.get(null, runOutput.getUris().get("kestra.mongo_types")), Charsets.UTF_8);
        assertThat(employee.size(), is(14));
        assertThat(employee.stream().filter(o -> !((Boolean) o.get("deleted"))).count(), is(7L));

        FileSerde.reader(new BufferedReader(new InputStreamReader(storageInterface.get(null, runOutput.getUris().get("second.employeeTerritory")))), r -> employee.add((Map<String, Object>) r));
        assertThat(types.size(), is(6));
        assertThat(types.stream().filter(o -> !((Boolean) o.get("deleted"))).count(), is(4L));

        // rerun state will prevent new records
        runOutput = task.run(runContext);
        assertThat(runOutput.getSize(), is(0));
    }
}