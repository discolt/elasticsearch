package org.elasticsearch.vpack.store;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.vpack.store.StoreSettings.INDEX_STORE_REMOTE_SETTINGS;

public class DetachStoredIndices implements ClusterStateListener {

    // key:index
    // value:remote-cluster
    private Map<String, String> stored = new HashMap<>();

    @Inject
    public DetachStoredIndices(ClusterService clusterService) {
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ClusterState state = event.state();
        ImmutableOpenMap<String, IndexMetaData> indices = state.metaData().indices();
        indices.forEach(e -> {
                    String store = INDEX_STORE_REMOTE_SETTINGS.get(e.value.getSettings());
                    if (!Strings.isNullOrEmpty(store)) {
                        this.stored.put(e.key, store);
                    }
                }
        );
        this.stored.entrySet().removeIf(e -> !indices.containsKey(e.getKey()));
    }

    /**
     * Get remote store
     *
     * @return
     */
    public String getStore(String index) {
        return stored.get(index);
    }

    public boolean isEmpty() {
        return stored.isEmpty();
    }

    public boolean hasStore(String index) {
        return stored.containsKey(index);
    }

}
