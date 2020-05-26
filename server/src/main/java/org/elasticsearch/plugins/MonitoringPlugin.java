package org.elasticsearch.plugins;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public interface MonitoringPlugin {

    /**
     * Returns additional monitoring collector added by this plugin.
     */
    default List<CollectorSpec> getCollectors() {
        return Collections.EMPTY_LIST;
    }

    /**
     * Called before all collectors is loaded.
     *
     * @param collectors     all external collectors
     * @param client         client
     * @param clusterService clusterService
     */
    default void onCollectors(List<CollectorSpec> collectors, Client client, ClusterService clusterService) {
    }

    /**
     * Collector Specification
     */
    interface CollectorSpec {

        /**
         * name
         *
         * @return name of collector
         */
        String name();

        /**
         * Collector execution
         */
        Collection<ToXContentObject> doCollect(Client client) throws Exception;

        /**
         * Indicates if the current collector is allowed to collect data
         *
         * @param isElectedMaster true if the current local node is the elected master node
         */
        default boolean shouldCollect(final boolean isElectedMaster) {
            return isElectedMaster;
        }

        /**
         * Collector execution timeout
         */
        default TimeValue timeout() {
            return TimeValue.timeValueSeconds(10);
        }
    }

}


