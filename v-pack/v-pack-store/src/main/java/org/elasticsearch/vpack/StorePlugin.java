/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.vpack;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.vpack.store.*;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Arrays.asList;
import static org.elasticsearch.vpack.store.StoreSettings.INDEX_STORE_REMOTE_SETTINGS;


public class StorePlugin extends Plugin implements EnginePlugin, ActionPlugin {

    private final SetOnce<MetaSyncService> syncedRemoteServiceSetOnce = new SetOnce<>();
    private final SetOnce<Client> clientSetOnceSetOnce = new SetOnce<>();
    private final SetOnce<DetachStoredIndices> storeDetachedIndicesSetOnce = new SetOnce<>();
    private final SetOnce<ThreadPool> threadPoolSetOnce = new SetOnce<>();
    private final SetOnce<StoreSettings> storeSettingsSetOnce = new SetOnce<>();

    @Override
    public List<ActionFilter> getActionFilters() {
        return asList(new FetchStoredFilter(clientSetOnceSetOnce.get(), storeDetachedIndicesSetOnce.get()));
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        clientSetOnceSetOnce.set(client);
        threadPoolSetOnce.set(threadPool);
        syncedRemoteServiceSetOnce.set(new MetaSyncService(client, threadPool));
        storeDetachedIndicesSetOnce.set(new DetachStoredIndices(clusterService));
        storeSettingsSetOnce.set(new StoreSettings(client.settings(), clusterService));
        return Collections.EMPTY_LIST;
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(final IndexSettings indexSettings) {
        String remoteCluster = INDEX_STORE_REMOTE_SETTINGS.get(indexSettings.getSettings());
        return Strings.isNullOrEmpty(remoteCluster)
                ? Optional.empty()
                : Optional.of(engineConfig -> new DetachStoredEngine(engineConfig, clientSetOnceSetOnce.get(), threadPoolSetOnce.get(), storeSettingsSetOnce.get()));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return StoreSettings.getSettings();
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        indexModule.addIndexEventListener(syncedRemoteServiceSetOnce.get());
    }

}
