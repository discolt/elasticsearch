package org.elasticsearch.vpack.xdcr.task.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.persistent.PersistentTaskParams;

import java.io.IOException;

/**
 * 索引复制任务参数
 */
public class ClusterSyncTaskParams implements PersistentTaskParams {

    public static final String NAME = "vpack/xdcr/sync/cluster";

    static final ParseField REPOSITORY_FIELD = new ParseField("repository");
    static final ParseField EXCLUDES_FIELD = new ParseField("excludes");

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<ClusterSyncTaskParams, Void> PARSER =
            new ConstructingObjectParser<>(NAME, (a) -> new ClusterSyncTaskParams((String) a[0], (String) a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), REPOSITORY_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), EXCLUDES_FIELD);
    }

    private final String repository;
    private final String excludes;

    public ClusterSyncTaskParams(String repository, String excludes) {
        this.repository = repository;
        this.excludes = excludes;
    }

    public String repository() {
        return repository;
    }

    public String excludes() {
        return excludes;
    }


    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT;
    }

    public ClusterSyncTaskParams(StreamInput in) throws IOException {
        this.repository = in.readString();
        this.excludes = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(repository);
        out.writeString(excludes);
    }

    public static ClusterSyncTaskParams fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("repository", repository);
        builder.field("excludes", excludes);
        builder.endObject();
        return builder;
    }

    public String toString() {
        return repository;
    }
}
