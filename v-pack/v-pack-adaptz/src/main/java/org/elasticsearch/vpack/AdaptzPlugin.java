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

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.vpack.adaptz.bulk.RestBulkProxyAction;
import org.elasticsearch.vpack.adaptz.bulk.action.IndicesAppendOnlyExistsAction;
import org.elasticsearch.vpack.adaptz.bulk.action.TransportIndicesAppendOnlyExistsAction;
import org.elasticsearch.vpack.adaptz.mget.RestMultiGetProxyAction;
import org.elasticsearch.vpack.adaptz.msearch.RestMultiSearchProxyAction;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class AdaptzPlugin extends Plugin implements ActionPlugin {

    public String name() {
        return "elasticsearch-adapt-zsearch";
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings,
                                             RestController restController,
                                             ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings,
                                             SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {

        List<RestHandler> list = new ArrayList<>();
        list.add(new RestBulkProxyAction(settings, restController));
        list.add(new RestMultiGetProxyAction(settings, restController));
        list.add(new RestMultiSearchProxyAction(settings, restController));
        return list;
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = new ArrayList<>();
        actions.add(new ActionHandler<>(IndicesAppendOnlyExistsAction.INSTANCE, TransportIndicesAppendOnlyExistsAction.class));
        return actions;
    }
}
