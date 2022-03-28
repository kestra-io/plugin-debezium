package io.kestra.plugin.debezium;

import io.kestra.core.models.annotations.PluginProperty;
import io.micronaut.core.annotation.Introspected;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;

import java.time.Duration;
import java.util.Map;
import javax.validation.constraints.NotNull;

public interface AbstractDebeziumInterface {
    @Schema(
        title = "The format of output",
        description = " Possible settings are:\n" +
            "- `RAW`: Send raw data from debezium.\n" +
            "- `INLINE`: Send a row like in the source with only data (remove after & before), all the cols will be present on each rows.\n" +
            "- `WRAP`: Send a row like INLINE but wrapped on a `record` field.\n"
    )
    @PluginProperty(dynamic = false)
    @NotNull
    AbstractDebeziumTask.Format getFormat();

    @Schema(
        title = "How to handle deleted rows",
        description = " Possible settings are:\n" +
            "- `ADD_FIELD`: add a deleted fields as boolean.\n" +
            "- `NULL`: send a row will all values as null.\n" +
            "- `DROP`: don't send row deleted."
    )
    @PluginProperty(dynamic = false)
    @NotNull
    AbstractDebeziumTask.Deleted getDeleted();

    @Schema(
        title = "The name of deleted fields if deleted is `ADD_FIELD`"
    )
    @PluginProperty(dynamic = false)
    @NotNull
    String getDeletedFieldName();

    @Schema(
        title = "How to handle key",
        description = " Possible settings are:\n" +
            "- `ADD_FIELD`: add key(s) merged with cols.\n" +
            "- `DROP`: drop keys."
    )
    @PluginProperty(dynamic = false)
    @NotNull
    AbstractDebeziumTask.Key getKey();

    @Schema(
        title = "How to handle metadata",
        description = " Possible settings are:\n" +
            "- `ADD_FIELD`: add metadata in a col named `metadata`.\n" +
            "- `DROP`: drop keys."
    )
    @PluginProperty(dynamic = false)
    @NotNull
    AbstractDebeziumTask.Metadata getMetadata();

    @Schema(
        title = "The name of metadata fields if metadata is `ADD_FIELD`"
    )
    @PluginProperty(dynamic = false)
    @NotNull
    String getMetadataFieldName();

    @Schema(
        title = "Split table on separate output `uris`",
        description = " Possible settings are:\n" +
            "- `TABLE`: will split all rows by tables on output with name `database.table`\n" +
            "- `DATABASE`: will split all rows by database on output with name `database`.\n" +
            "- `OFF`: will **NOT** split all rows resulting a single `data` output."
    )
    @PluginProperty(dynamic = false)
    @NotNull
    AbstractDebeziumTask.SplitTable getSplitTable();

    @Schema(
        title = "Ignore ddl statement",
        description = "Ignore create table and others administration operations"
    )
    @PluginProperty(dynamic = false)
    @NotNull
    Boolean getIgnoreDdl();

    @Schema(
        title = "Hostname of the remote server"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getHostname();

    @Schema(
        title = "Port of the remote server"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getPort();

    @Schema(
        title = "Username on the remote server"
    )
    @PluginProperty(dynamic = true)
    String getUsername();

    @Schema(
        title = "Password on the remote server"
    )
    @PluginProperty(dynamic = true)
    String getPassword();

    @Schema(
        title = "An optional, comma-separated list of regular expressions that match the names of the databases for which to capture changes.",
        description = "The connector does not capture changes in any database whose name is not in `includedDatabases``. By default, the connector captures changes in all databases. Do not also set the `excludedDatabases` connector configuration property."
    )
    @PluginProperty(dynamic = true)
    Object getIncludedDatabases();

    @Schema(
        title = "An optional, comma-separated list of regular expressions that match the names of databases for which you do not want to capture changes. ",
        description = "The connector captures changes in any database whose name is not in the `excludedDatabases``. Do not also set the `includedDatabases` connector configuration property."
    )
    @PluginProperty(dynamic = true)
    Object getExcludedDatabases();

    @Schema(
        title = "An optional, comma-separated list of regular expressions that match fully-qualified table identifiers of tables whose changes you want to capture.",
        description = "The connector does not capture changes in any table not included in `includedTables``. Each identifier is of the form databaseName.tableName. By default, the connector captures changes in every non-system table in each database whose changes are being captured. Do not also specify the `excludedTables` connector configuration property."
    )
    @PluginProperty(dynamic = true)
    Object getIncludedTables();

    @Schema(
        title = "An optional, comma-separated list of regular expressions that match fully-qualified table identifiers for tables whose changes you do not want to capture.",
        description = "The connector captures changes in any table not included in `excludedTables`. Each identifier is of the form databaseName.tableName. Do not also specify the `includedTables` connector configuration property."
    )
    @PluginProperty(dynamic = true)
    Object getExcludedTables();


    @Schema(
        title = "An optional, comma-separated list of regular expressions that match the fully-qualified names of columns to exclude from change event record values.",
        description = "Fully-qualified names for columns are of the form databaseName.tableName.columnName."
    )
    @PluginProperty(dynamic = true)
    Object getIncludedColumns();

    @Schema(
        title = "An optional, comma-separated list of regular expressions that match the fully-qualified names of columns to include in change event record values.",
        description = "Fully-qualified names for columns are of the form databaseName.tableName.columnName."
    )
    @PluginProperty(dynamic = true)
    Object getExcludedColumns();

    @Schema(
        title = "Additional configuration properties",
        description = "Any additional configuration properties that is valid for the current driver"
    )
    @PluginProperty(dynamic = true)
    Map<String, String> getProperties();

    @Schema(
        title = "The name of Debezium state file"
    )
    @PluginProperty(dynamic = false)
    @NotNull
    String getStateName();

    @Schema(
        title = "The max number of rows to fetch before stopping",
        description = "It's not an hard limit and is evaluated every second"
    )
    @PluginProperty(dynamic = false)
    Integer getMaxRecords();

    @Schema(
        title = "The max total processing duration",
        description = "It's not an hard limit and is evaluated every second"
    )
    @PluginProperty(dynamic = false)
    Duration getMaxDuration();

    @Schema(
        title = "The max duration waiting for new rows",
        description = "It's not an hard limit and is evaluated every second"
    )
    @PluginProperty(dynamic = false)
    Duration getMaxWait();
}
