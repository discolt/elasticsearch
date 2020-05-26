package org.elasticsearch.xpack.monitoring.collector;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.plugins.MonitoringPlugin.*;

public class CollectorWrapper extends Collector {

    private final CollectorSpec spec;
    private final Client client;

    public CollectorWrapper(CollectorSpec spec, Client client, ClusterService clusterService) {
        super(spec.name(), clusterService, SPEC_DEFAULT_TIMEOUT, null);
        this.spec = spec;
        this.client = client;
    }

    @Override
    protected boolean shouldCollect(final boolean isElectedMaster) {
        return spec.shouldCollect(isElectedMaster);
    }

    @Override
    protected Collection<MonitoringDoc> doCollect(MonitoringDoc.Node node, long interval, ClusterState clusterState) throws Exception {
        Collection<ToXContentObject> docs = spec.doCollect(client);
        final String clusterUuid = clusterUuid(clusterState);
        final List<MonitoringDoc> results = new ArrayList<>();
        docs.forEach(original -> results.add(new DocWrapper(original, clusterUuid, timestamp(), interval, node, MonitoredSystem.ES, name(), null)));
        return results;
    }

    class DocWrapper extends MonitoringDoc {
        final ToXContentObject original;

        public DocWrapper(ToXContentObject original, String cluster, long timestamp, long intervalMillis, Node node, MonitoredSystem system, String type, String id) {
            super(cluster, timestamp, intervalMillis, node, system, type, id);
            this.original = original;
        }

        @Override
        protected void innerToXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(name());
            original.toXContent(builder, params);
            builder.endObject();
        }
    }

    public static final Setting<TimeValue> SPEC_DEFAULT_TIMEOUT = collectionTimeoutSetting("spec.collector.timeout");
}

