/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy notNull the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.vpack;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.*;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.vpack.xdcr.ActionFilterPlugin;
import org.elasticsearch.vpack.xdcr.action.cluster.StartClusterSyncAction;
import org.elasticsearch.vpack.xdcr.action.cluster.StopClusterSyncAction;
import org.elasticsearch.vpack.xdcr.action.index.*;
import org.elasticsearch.vpack.xdcr.action.index.bulk.BulkShardOperationsAction;
import org.elasticsearch.vpack.xdcr.action.index.bulk.TransportBulkShardOperationsAction;
import org.elasticsearch.vpack.xdcr.action.stats.StatsAction;
import org.elasticsearch.vpack.xdcr.engine.FollowingEngineFactory;
import org.elasticsearch.vpack.xdcr.rest.*;
import org.elasticsearch.vpack.xdcr.task.cluster.ClusterSyncExecutor;
import org.elasticsearch.vpack.xdcr.task.cluster.ClusterSyncTaskParams;
import org.elasticsearch.vpack.xdcr.task.index.IndexSyncExecutor;
import org.elasticsearch.vpack.xdcr.task.index.IndexSyncTaskParams;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

public class XDCRPlugin extends Plugin implements ActionPlugin, PersistentTaskPlugin, EnginePlugin {

    public static final String XDCR_THREAD_POOL_NAME = "xdcr";

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
                new ActionHandler<>(StartIndexSyncAction.INSTANCE, StartIndexSyncAction.Transport.class),
                new ActionHandler<>(StopIndexSyncAction.INSTANCE, StopIndexSyncAction.Transport.class),
                new ActionHandler<>(ShardDeliveryAction.INSTANCE, ShardDeliveryAction.TransportAction.class),
                new ActionHandler<>(BulkShardOperationsAction.INSTANCE, TransportBulkShardOperationsAction.class),
                new ActionHandler<>(StartClusterSyncAction.INSTANCE, StartClusterSyncAction.Transport.class),
                new ActionHandler<>(StopClusterSyncAction.INSTANCE, StopClusterSyncAction.Transport.class),
                new ActionHandler<>(PutIndexAction.INSTANCE, PutIndexAction.Transport.class),
                new ActionHandler<>(StatsAction.INSTANCE, StatsAction.Transport.class)
        );
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        return singletonList(new ActionFilterPlugin());
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        return Arrays.asList(
                new RestStatsAction(settings, restController),
                new RestStatsCatAction(settings, restController),
                new RestStartIndexSyncAction(settings, restController),
                new RestStopIndexSyncAction(settings, restController),
                new RestStartClusterSyncAction(settings, restController),
                new RestStopClusterSyncAction(settings, restController));

    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(ClusterService clusterService,
                                                                       ThreadPool threadPool,
                                                                       Client client,
                                                                       SettingsModule settingsModule) {
        return Arrays.asList(
                new IndexSyncExecutor(clusterService, threadPool, client),
                new ClusterSyncExecutor(clusterService, threadPool, client)
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Arrays.asList(
                new NamedWriteableRegistry.Entry(PersistentTaskParams.class, IndexSyncTaskParams.NAME, IndexSyncTaskParams::new),
                new NamedWriteableRegistry.Entry(PersistentTaskParams.class, ClusterSyncTaskParams.NAME, ClusterSyncTaskParams::new)
        );
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Arrays.asList(
                new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(IndexSyncTaskParams.NAME), IndexSyncTaskParams::fromXContent),
                new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(ClusterSyncTaskParams.NAME), ClusterSyncTaskParams::fromXContent)
        );
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return Collections.singletonList(new FixedExecutorBuilder(settings, XDCR_THREAD_POOL_NAME, 32, 100, "vpack.xdcr.thread_pool"));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(XDCR_FOLLOWING_INDEX_SETTING);
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(final IndexSettings indexSettings) {
        if (XDCR_FOLLOWING_INDEX_SETTING.get(indexSettings.getSettings())) {
            return Optional.of(new FollowingEngineFactory());
        } else {
            return Optional.empty();
        }
    }


    public static final Setting<Boolean> XDCR_FOLLOWING_INDEX_SETTING =
            Setting.boolSetting("index.vpack.xdcr.following_index", false, Setting.Property.IndexScope, Setting.Property.InternalIndex);


}
