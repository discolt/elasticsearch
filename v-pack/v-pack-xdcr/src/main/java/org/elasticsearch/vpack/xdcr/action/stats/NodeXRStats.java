package org.elasticsearch.vpack.xdcr.action.stats;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.vpack.xdcr.core.ChunkSentStat;

import java.io.IOException;
import java.util.List;

public class NodeXRStats extends BaseNodeResponse implements ToXContentFragment {

    private List<ChunkSentStat> records;

    NodeXRStats() {
    }

    public NodeXRStats(DiscoveryNode node, @Nullable List<ChunkSentStat> records) {
        super(node);
        this.records = records;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        this.records = in.readList(ChunkSentStat::read);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(records);
    }

    public static NodeXRStats readNodeStats(StreamInput in) throws IOException {
        NodeXRStats stats = new NodeXRStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("name", getNode().getName());
        if (records != null && !records.isEmpty()) {
            builder.startObject("trackers");
            for (ChunkSentStat stat : records) {
                stat.toXContent(builder, params);
            }
            builder.endObject();
        }
        return builder;
    }
}
