package org.elasticsearch.vpack.xdcr.task.index;

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
public class IndexSyncTaskParams implements PersistentTaskParams {

    public static final String NAME = "vpack/xdcr/sync/index";

    static final ParseField REPOSITORY_FIELD = new ParseField("repository");
    static final ParseField INDEX_FIELD = new ParseField("index");

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<IndexSyncTaskParams, Void> PARSER =
            new ConstructingObjectParser<>(NAME, (a) -> new IndexSyncTaskParams((String) a[0], (String) a[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), REPOSITORY_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_FIELD);
    }

    private final String repository;
    private final String index;

    public IndexSyncTaskParams(String repository, String index) {
        this.repository = repository;
        this.index = index;
    }

    public String repository() {
        return repository;
    }

    public String index() {
        return index;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT;
    }

    public IndexSyncTaskParams(StreamInput in) throws IOException {
        this.repository = in.readString();
        this.index = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(repository);
        out.writeString(index);
    }

    public static IndexSyncTaskParams fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("repository", repository);
        builder.field("index", index);
        builder.endObject();
        return builder;
    }

    public String toString() {
        return repository + ":" + index;
    }
}
