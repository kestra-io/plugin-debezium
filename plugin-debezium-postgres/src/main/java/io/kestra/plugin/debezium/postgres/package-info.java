@PluginSubGroup(
    description = "This sub-group of plugins contains tasks for using Debezium with PostgreSQL.\n" +
        "Debezium is an open source distributed platform for change data capture.",
    categories = PluginSubGroup.PluginCategory.DATABASE,
    categories = {
        PluginSubGroup.PluginCategory.DATA
    }
)
package io.kestra.plugin.debezium.postgres;

import io.kestra.core.models.annotations.PluginSubGroup;