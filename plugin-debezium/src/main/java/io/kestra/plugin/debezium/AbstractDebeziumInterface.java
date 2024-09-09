package io.kestra.plugin.debezium;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import java.time.Duration;
import java.util.Map;
import jakarta.validation.constraints.NotNull;

public interface AbstractDebeziumInterface {
    @Schema(
        title = "The format of the output.",
        description = " Possible settings are:\n" +
            "- `RAW`: Send raw data from debezium.\n" +
            "- `INLINE`: Send a row like in the source with only data (remove after & before), all the columns will be present for each row.\n" +
            "- `WRAP`: Send a row like INLINE but wrapped in a `record` field.\n"
    )
    @PluginProperty
    @NotNull
    AbstractDebeziumTask.Format getFormat();

    @Schema(
        title = "Specify how to handle deleted rows.",
        description = " Possible settings are:\n" +
            "- `ADD_FIELD`: Add a deleted field as boolean.\n" +
            "- `NULL`: Send a row with all values as null.\n" +
            "- `DROP`: Don't send deleted row."
    )
    @PluginProperty
    @NotNull
    AbstractDebeziumTask.Deleted getDeleted();

    @Schema(
        title = "The name of deleted field if deleted is `ADD_FIELD`."
    )
    @PluginProperty
    @NotNull
    String getDeletedFieldName();

    @Schema(
        title = "Specify how to handle key.",
        description = " Possible settings are:\n" +
            "- `ADD_FIELD`: Add key(s) merged with columns.\n" +
            "- `DROP`: Drop keys."
    )
    @PluginProperty
    @NotNull
    AbstractDebeziumTask.Key getKey();

    @Schema(
        title = "Specify how to handle metadata.",
        description = " Possible settings are:\n" +
            "- `ADD_FIELD`: Add metadata in a column named `metadata`.\n" +
            "- `DROP`: Drop metadata."
    )
    @PluginProperty
    @NotNull
    AbstractDebeziumTask.Metadata getMetadata();

    @Schema(
        title = "The name of metadata field if metadata is `ADD_FIELD`."
    )
    @PluginProperty
    @NotNull
    String getMetadataFieldName();

    @Schema(
        title = "Split table on separate output `uris`.",
        description = " Possible settings are:\n" +
            "- `TABLE`: This will split all rows by tables on output with name `database.table`\n" +
            "- `DATABASE`: This will split all rows by databases on output with name `database`.\n" +
            "- `OFF`: This will **NOT** split all rows resulting in a single `data` output."
    )
    @PluginProperty
    @NotNull
    AbstractDebeziumTask.SplitTable getSplitTable();

    @Schema(
        title = "Ignore DDL statement.",
        description = "Ignore CREATE, ALTER, DROP and TRUNCATE operations."
    )
    @PluginProperty
    @NotNull
    Boolean getIgnoreDdl();

    @Schema(
        title = "Hostname of the remote server."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getHostname();

    @Schema(
        title = "Port of the remote server."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    String getPort();

    @Schema(
        title = "Username on the remote server."
    )
    @PluginProperty(dynamic = true)
    String getUsername();

    @Schema(
        title = "Password on the remote server."
    )
    @PluginProperty(dynamic = true)
    String getPassword();

    @Schema(
        title = "An optional, comma-separated list of regular expressions that match the names of the databases for which to capture changes.",
        description = "The connector does not capture changes in any database whose name is not in `includedDatabases`. By default, the connector captures changes in all databases. Do not also set the `excludedDatabases` connector configuration property."
    )
    @PluginProperty(dynamic = true)
    Object getIncludedDatabases();

    @Schema(
        title = "An optional, comma-separated list of regular expressions that match the names of databases for which you do not want to capture changes.",
        description = "The connector captures changes in any database whose name is not in the `excludedDatabases`. Do not also set the `includedDatabases` connector configuration property."
    )
    @PluginProperty(dynamic = true)
    Object getExcludedDatabases();

    @Schema(
        title = "An optional, comma-separated list of regular expressions that match fully-qualified table identifiers of tables whose changes you want to capture.",
        description = "The connector does not capture changes in any table not included in `includedTables`. Each identifier is of the form databaseName.tableName. By default, the connector captures changes in every non-system table in each database whose changes are being captured. Do not also specify the `excludedTables` connector configuration property."
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
        description = "Fully-qualified names for columns are of the form databaseName.tableName.columnName. Do not also specify the `excludedColumns` connector configuration property."
    )
    @PluginProperty(dynamic = true)
    Object getIncludedColumns();

    @Schema(
        title = "An optional, comma-separated list of regular expressions that match the fully-qualified names of columns to include in change event record values.",
        description = "Fully-qualified names for columns are of the form databaseName.tableName.columnName. Do not also specify the `includedColumns` connector configuration property.\""
    )
    @PluginProperty(dynamic = true)
    Object getExcludedColumns();

    @Schema(
        title = "Additional configuration properties.",
        description = "Any additional configuration properties that is valid for the current driver."
    )
    @PluginProperty(dynamic = true)
    Map<String, String> getProperties();

    @Schema(
        title = "The name of the Debezium state file stored in the KV Store for that namespace."
    )
    @PluginProperty
    @NotNull
    String getStateName();
}
