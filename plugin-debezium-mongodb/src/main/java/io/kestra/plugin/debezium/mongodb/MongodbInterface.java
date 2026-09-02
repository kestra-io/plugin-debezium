package io.kestra.plugin.debezium.mongodb;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface MongodbInterface {

    @Schema(
        title = "Connection string to a MongoDB replica set or sharded cluster",
        examples = {
            "mongodb://mongo_user:<password>@127.0.0.1:27017/?replicaSet=rs0",
            "mongodb://mongo_user:<password>@mongos0.example.com:27017,mongos1.example.com:27017/"
        }
    )
    @PluginProperty(group = "connection", secret = true)
    Property<String> getConnectionString();

    @Schema(
        title = "The name of the MongoDB database collection included from which to stream the changes",
        description = "A list of regular expressions that match the collection namespaces (for example, <dbName>.<collectionName>) of all collections to be monitored",
        example = "inventory[.]*"
    )
    @PluginProperty(dynamic = true, group = "advanced")
    Object getIncludedCollections();

    @Schema(
        title = "The name of the MongoDB database collection excluded from which to stream the changes",
        description = "A list of regular expressions that match the collection namespaces (for example, <dbName>.<collectionName>) of all collections to be excluded",
        example = "inventory[.]*"
    )
    @PluginProperty(dynamic = true, group = "advanced")
    Object getExcludedCollections();

    @Schema(
        title = "Specifies the criteria for running a snapshot when the connector starts",
        description = " Possible settings are:\n" +
            "- `INITIAL`: The connector runs a snapshot only when no offsets have been recorded for the logical server name.\n" +
            "- `INITIAL_ONLY`: The connector runs a snapshot only when no offsets have been recorded for the logical server name and then stops; i.e. it will not stream change events from the oplog.\n"
            +
            "- `NO_DATA`: The connector captures the structure of all relevant collections, performing all the steps described in the default snapshot workflow, except that it does not create READ events to represent the data set at the point of the connector’s start-up.\n"
            +
            "- `WHEN_NEEDED`: The connector runs a snapshot upon startup whenever it deems it necessary. That is, when no offsets are available, or when a previously recorded offset specifies an oplog position that is not available on the server."
    )
    @NotNull
    @PluginProperty(group = "main")
    Property<SnapshotMode> getSnapshotMode();

    public enum SnapshotMode {
        INITIAL,
        INITIAL_ONLY,
        NO_DATA,
        WHEN_NEEDED
    }
}
