package org.elasticsearch.vpack.xdcr.core.follower;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.vpack.xdcr.util.SettingUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class UpdateIndexMetaDataOnFollower {

    private final Logger logger;
    private final ClusterService clusterService;
    private final Client client;
    private final IndexScopedSettings indexScopedSettings;

    public UpdateIndexMetaDataOnFollower(ClusterService clusterService,
                                         Client client,
                                         IndexScopedSettings indexScopedSettings) {
        this.logger = Loggers.getLogger(getClass());
        this.clusterService = clusterService;
        this.client = client;
        this.indexScopedSettings = indexScopedSettings;
    }

    public void createOrUpdate(IndexMetaData sourceMetaData) {
        Consumer<Exception> errorHandler = e -> {
            logger.error(e);
        };

        MetaData localMetaData = clusterService.state().metaData();
        String index = sourceMetaData.getIndex().getName();
        if (!localMetaData.hasIndex(index)) {
            logger.trace("[xdcr] create index {}", sourceMetaData);
            create(sourceMetaData, errorHandler);
        } else {
            logger.trace("[xdcr] update index {}", sourceMetaData);
            update(sourceMetaData, localMetaData.index(index), errorHandler);
        }

    }

    private void create(IndexMetaData sourceIndexMetaData, Consumer<Exception> errorHandler) {

        CreateIndexRequest createIndexRequest = new CreateIndexRequest();
        createIndexRequest.cause("sync-remote-index");
        createIndexRequest.index(sourceIndexMetaData.getIndex().getName());

        // clone alias
        if (sourceIndexMetaData.getAliases() != null && !sourceIndexMetaData.getAliases().isEmpty()) {
            sourceIndexMetaData.getAliases().forEach(e -> {
                Alias alias = new Alias(e.value.alias());
                if (e.value.indexRouting() != null) {
                    alias.routing(e.value.indexRouting());
                }
                if (e.value.searchRouting() != null) {
                    alias.searchRouting(e.value.searchRouting());
                }
                if (e.value.filter() != null) {
                    alias.filter(e.value.filter().toString());
                }
                createIndexRequest.alias(alias);
            });
        }

        // clone settings
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(sourceIndexMetaData.getSettings());
        settingsBuilder.remove(IndexMetaData.SETTING_INDEX_UUID);
        settingsBuilder.remove(IndexMetaData.SETTING_CREATION_DATE);
        settingsBuilder.remove(IndexMetaData.SETTING_VERSION_CREATED);
        settingsBuilder.remove(IndexMetaData.SETTING_INDEX_PROVIDED_NAME);
        createIndexRequest.settings(settingsBuilder.build());

        // clone mappings
        for (ObjectObjectCursor<String, MappingMetaData> entry : sourceIndexMetaData.getMappings()) {
            createIndexRequest.mapping(entry.key, entry.value.getSourceAsMap());
        }
        createIndexRequest.masterNodeTimeout(TimeValue.timeValueSeconds(5));
        client.admin().indices().create(createIndexRequest, ActionListener.wrap(null, errorHandler));
    }

    private void update(IndexMetaData sourceIndexMetaData, IndexMetaData localIndexMetaData, Consumer<Exception> errorHandler) {
        String index = localIndexMetaData.getIndex().getName();

        /* ===================================
         * ------- update settings -----------
         * ===================================
         */
        Settings sourceSettings = SettingUtils.filter(sourceIndexMetaData.getSettings());
        Settings localSettings = SettingUtils.filter(localIndexMetaData.getSettings());
        final Settings diffSettings = sourceSettings.filter(
                s -> (localSettings.get(s) == null || localSettings.get(s).equals(sourceSettings.get(s)) == false)
        );
        Settings dynamicUpdatedSettings = diffSettings.filter(indexScopedSettings::isDynamicSetting);
        Settings reopenUpdatedSettings = diffSettings.filter(key -> !indexScopedSettings.isDynamicSetting(key));
        if (!dynamicUpdatedSettings.isEmpty()) {
            final UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index).settings(dynamicUpdatedSettings);
            client.admin().indices().updateSettings(updateSettingsRequest, ActionListener.wrap(null, errorHandler));
        }
        if (!reopenUpdatedSettings.isEmpty()) {
            CloseIndexRequest closeRequest = new CloseIndexRequest().indices(index);
            CheckedConsumer<CloseIndexResponse, Exception> updatingHandler = response -> {
                final UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index).settings(reopenUpdatedSettings);
                client.admin().indices().updateSettings(updateSettingsRequest, ActionListener.wrap(r -> {
                    OpenIndexRequest openIndexRequest = new OpenIndexRequest(index);
                    client.admin().indices().open(openIndexRequest, ActionListener.wrap(null, errorHandler));
                }, fail -> {
                    logger.error("[xdcr]update settings failed", fail);
                    OpenIndexRequest openIndexRequest = new OpenIndexRequest(index);
                    client.admin().indices().open(openIndexRequest, ActionListener.wrap(null, errorHandler));
                }));
            };
            client.admin().indices().close(closeRequest, ActionListener.wrap(updatingHandler, errorHandler));
        }

        /* ===================================
         * ------- update aliases -----------
         * ===================================
         */
        if (!sourceIndexMetaData.getAliases().equals(localIndexMetaData.getAliases())) {
            IndicesAliasesRequest aliasesRequest = compareAlias(index, sourceIndexMetaData.getAliases(), localIndexMetaData.getAliases());
            if (!aliasesRequest.getAliasActions().isEmpty()) {
                client.admin().indices().aliases(aliasesRequest, ActionListener.wrap(null, errorHandler));
            }
        }

        /* ===================================
         * ------- update mappings -----------
         * ===================================
         */
        if (!sourceIndexMetaData.getMappings().equals(localIndexMetaData.getMappings())) {
            PutMappingRequest putMappingRequest = new PutMappingRequest(sourceIndexMetaData.getIndex().getName());
            for (ObjectObjectCursor<String, MappingMetaData> entry : sourceIndexMetaData.getMappings()) {
                putMappingRequest.type(entry.key);
                putMappingRequest.source(entry.value.getSourceAsMap());
                client.admin().indices().putMapping(putMappingRequest, ActionListener.wrap(null, errorHandler));
            }
        }
    }

    private static IndicesAliasesRequest compareAlias(String index, ImmutableOpenMap<String, AliasMetaData> source, ImmutableOpenMap<String, AliasMetaData> dest) {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        source.forEach(e -> {
            if (!dest.containsKey(e.key)) {
                AliasActions aliasAction = new AliasActions(AliasActions.Type.ADD);
                aliasAction.index(index).alias(e.value.alias());
                if (e.value.indexRouting() != null) {
                    aliasAction.routing(e.value.indexRouting());
                }
                if (e.value.searchRouting() != null) {
                    aliasAction.searchRouting(e.value.searchRouting());
                }
                if (e.value.filter() != null) {
                    aliasAction.filter(e.value.filter().toString());
                }
                request.addAliasAction(aliasAction);
            }
        });
        dest.forEach(e -> {
            if (!source.containsKey(e.key)) {
                AliasActions aliasAction = new AliasActions(AliasActions.Type.REMOVE);
                aliasAction.index(index).alias(e.value.alias());
                request.addAliasAction(aliasAction);
            }
        });
        return request;
    }

}
