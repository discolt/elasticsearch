package org.elasticsearch.vpack.xdcr.task.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.vpack.xdcr.action.index.PutIndexAction;
import org.elasticsearch.vpack.xdcr.action.index.StopIndexSyncAction;
import org.elasticsearch.vpack.xdcr.action.index.IndexLevelRequest;
import org.elasticsearch.vpack.xdcr.utils.SettingUtils;

import java.util.Optional;

import static org.elasticsearch.common.settings.IndexScopedSettings.BUILT_IN_INDEX_SETTINGS;
import static org.elasticsearch.vpack.xdcr.action.index.PutIndexAction.IMD_CUSTOM_XDCR;
import static org.elasticsearch.vpack.xdcr.action.index.PutIndexAction.IMD_CUSTOM_XDCR_KEY_LEADER_INDEX_UUID;
import static org.elasticsearch.vpack.xdcr.utils.Actions.*;
import static org.elasticsearch.vpack.xdcr.utils.Utils.remoteClusterPrepared;
import static org.elasticsearch.vpack.xdcr.utils.Utils.sleep;

public class IndexMetaDataSyncThread implements Runnable {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(3);
    private static final Logger logger = LogManager.getLogger(IndexMetaDataSyncThread.class);
    private static final IndexScopedSettings DEFAULT_SCOPED_SETTINGS = new IndexScopedSettings(Settings.EMPTY, BUILT_IN_INDEX_SETTINGS);

    private ClusterService clusterService;
    private Client client;
    private Client remote;
    private IndexSyncTaskParams params;

    public IndexMetaDataSyncThread(ClusterService clusterService, Client client, IndexSyncTaskParams params) {
        this.clusterService = clusterService;
        this.client = client;
        this.params = params;
    }

    @Override
    public void run() {
        if (!remoteClusterPrepared(client, params.repository())) {
            sleep(5);
            return;
        }

        remote = client.getRemoteClusterClient(params.repository());
        String index = params.index();
        IndexMetaData leadIndexMetaData = clusterService.state().metaData().index(index);

        // 如果本地不存在，则删除远程索引并停止任务
        if (leadIndexMetaData == null) {
            deleteAndStopSyncRemoteIndex(index);
            return;
        }

        // 远端不存在则创建
        boolean exists = remote.admin().indices().prepareExists(index).get(TIMEOUT).isExists();
        if (!exists) {
            remote.execute(PutIndexAction.INSTANCE, new PutIndexAction.Request(leadIndexMetaData)).actionGet();
            return;
        }

        // 检查Index_UUID是否一致， 不一致则删除并停止同步
        String currentIndexUUID = leadIndexMetaData.getIndex().getUUID();
        String remoteIndexUUID = Optional.ofNullable(remote.admin().cluster().prepareState().get(TIMEOUT)
                .getState().metaData().index(index).getCustomData(IMD_CUSTOM_XDCR).get(IMD_CUSTOM_XDCR_KEY_LEADER_INDEX_UUID)).orElse(null);
        if (remoteIndexUUID != null && !currentIndexUUID.equals(remoteIndexUUID)) {
            deleteAndStopSyncRemoteIndex(index);
            return;
        }

        // open/close 状态同步
        IndexMetaData remoteMeta = remote.admin().cluster().prepareState().get().getState().metaData().index(index);
        IndexMetaData.State currentState = leadIndexMetaData.getState();
        IndexMetaData.State remoteState = remoteMeta.getState();
        if (!currentState.equals(remoteState)) {
            if (leadIndexMetaData.getState().equals(IndexMetaData.State.OPEN)) {
                remote.admin().indices().prepareOpen(index).execute().actionGet();
            } else {
                remote.admin().indices().prepareClose(index).execute().actionGet();
            }
            return;
        }

        // settings alias mappings syncing
        updateOthers(leadIndexMetaData, remoteMeta);
    }

    private void deleteAndStopSyncRemoteIndex(String index) {
        logger.warn("delete and stop sync for index {}", index);
        IndexLevelRequest removeSyncRequest = new IndexLevelRequest(params.repository(), params.index());
        boolean remoteIndexExists = remote.admin().indices().prepareExists(index).get().isExists();
        if (remoteIndexExists) {
            AcknowledgedResponse response = remote.admin().indices().delete(new DeleteIndexRequest(params.index())).actionGet();
            if (response.isAcknowledged()) {
                client.execute(StopIndexSyncAction.INSTANCE, removeSyncRequest).actionGet();
            }
        } else {
            client.execute(StopIndexSyncAction.INSTANCE, removeSyncRequest).actionGet();
        }
    }

    private void updateOthers(IndexMetaData sourceIndexMetaData, IndexMetaData remoteIndexMetaData) {
        String index = sourceIndexMetaData.getIndex().getName();

        // aliases
        if (!sourceIndexMetaData.getAliases().equals(remoteIndexMetaData.getAliases())) {
            IndicesAliasesRequest aliasesRequest = compareAlias(index, sourceIndexMetaData.getAliases(), remoteIndexMetaData.getAliases());
            if (!aliasesRequest.getAliasActions().isEmpty()) {
                remote.admin().indices().aliases(aliasesRequest).actionGet();
            }
        }

        // mappings
        if (!sourceIndexMetaData.getMappings().equals(remoteIndexMetaData.getMappings())) {
            MappingMetaData mappingMetaData = sourceIndexMetaData.getMappings().iterator().next().value;
            PutMappingRequest putMappingRequest = new PutMappingRequest(index);
            putMappingRequest.type(mappingMetaData.type());
            putMappingRequest.source(mappingMetaData.source().string(), XContentType.JSON);
            remote.admin().indices().putMapping(putMappingRequest).actionGet();
        }

        // settings
        Settings sourceSettings = SettingUtils.filter(sourceIndexMetaData.getSettings());
        Settings localSettings = SettingUtils.filter(remoteIndexMetaData.getSettings());
        final Settings diffSettings = sourceSettings.filter(s -> (localSettings.get(s) == null || localSettings.get(s).equals(sourceSettings.get(s)) == false));
        Settings dynamicUpdatedSettings = diffSettings.filter(DEFAULT_SCOPED_SETTINGS::isDynamicSetting);
        Settings reopenUpdatedSettings = diffSettings.filter(key -> !DEFAULT_SCOPED_SETTINGS.isDynamicSetting(key));
        if (!dynamicUpdatedSettings.isEmpty()) {
            final UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index).settings(dynamicUpdatedSettings);
            remote.admin().indices().updateSettings(updateSettingsRequest).actionGet();
        }
        if (!reopenUpdatedSettings.isEmpty()) {
            final UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(index).settings(reopenUpdatedSettings);
            remote.admin().indices().close(new CloseIndexRequest(params.index()), success(
                    r -> remote.admin().indices().updateSettings(updateSettingsRequest,
                            always(k -> remote.admin().indices().prepareOpen(index).execute().actionGet())))
            );
        }
    }


    private static IndicesAliasesRequest compareAlias(String index, ImmutableOpenMap<String, AliasMetaData> source, ImmutableOpenMap<String, AliasMetaData> dest) {
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        source.forEach(e -> {
            if (!dest.containsKey(e.key)) {
                IndicesAliasesRequest.AliasActions aliasAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD);
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
                IndicesAliasesRequest.AliasActions aliasAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE);
                aliasAction.index(index).alias(e.value.alias());
                request.addAliasAction(aliasAction);
            }
        });
        return request;
    }

}
