package org.elasticsearch.vpack.limit;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.ingest.PipelineConfiguration;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.cluster.metadata.MetaData.Custom;
import static org.elasticsearch.cluster.metadata.MetaData.XContentContext;

public final class RateLimiterMetadata implements MetaData.Custom {

    public static final String TYPE = "tenant_limiter";
    private static final ParseField LIMITED_FIELD = new ParseField("limiters");
    private static final ObjectParser<List<RateLimiterConfiguration>, Void> LIMITER_METADATA_PARSER = new ObjectParser<>(
            "tenant_limiter_metadata", ArrayList::new);

    static {
        LIMITER_METADATA_PARSER.declareObjectArray(List::addAll, RateLimiterConfiguration.getParser(), LIMITED_FIELD);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    private final Map<String, RateLimiterConfiguration> rateLimiters;

    private RateLimiterMetadata() {
        this.rateLimiters = Collections.emptyMap();
    }

    public RateLimiterMetadata(Map<String, RateLimiterConfiguration> rateLimiters) {
        this.rateLimiters = Collections.unmodifiableMap(rateLimiters);
    }

    public Map<String, RateLimiterConfiguration> getRateLimiters() {
        return rateLimiters;
    }

    public RateLimiterMetadata(StreamInput in) throws IOException {
        int size = in.readVInt();
        Map<String, RateLimiterConfiguration> pipelines = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            RateLimiterConfiguration limiter = RateLimiterConfiguration.readFrom(in);
            pipelines.put(limiter.getTenant(), limiter);
        }
        this.rateLimiters = Collections.unmodifiableMap(pipelines);
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(rateLimiters.size());
        for (RateLimiterConfiguration pipeline : rateLimiters.values()) {
            pipeline.writeTo(out);
        }
    }

    public static RateLimiterMetadata fromXContent(XContentParser parser) throws IOException {
        Map<String, RateLimiterConfiguration> pipelines = new HashMap<>();
        List<RateLimiterConfiguration> configs = LIMITER_METADATA_PARSER.parse(parser, null);
        for (RateLimiterConfiguration rateLimiter : configs) {
            pipelines.put(rateLimiter.getTenant(), rateLimiter);
        }
        return new RateLimiterMetadata(pipelines);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(LIMITED_FIELD.getPreferredName());
        for (RateLimiterConfiguration limiter : rateLimiters.values()) {
            limiter.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public EnumSet<XContentContext> context() {
        return MetaData.ALL_CONTEXTS;
    }

    @Override
    public Diff<MetaData.Custom> diff(MetaData.Custom before) {
        return new RateLimiterMetadataDiff((RateLimiterMetadata) before, this);
    }

    public static NamedDiff<Custom> readDiffFrom(StreamInput in) throws IOException {
        return new RateLimiterMetadata.RateLimiterMetadataDiff(in);
    }

    static class RateLimiterMetadataDiff implements NamedDiff<MetaData.Custom> {

        final Diff<Map<String, RateLimiterConfiguration>> rateLimiters;

        RateLimiterMetadataDiff(RateLimiterMetadata before, RateLimiterMetadata after) {
            this.rateLimiters = DiffableUtils.diff(before.rateLimiters, after.rateLimiters, DiffableUtils.getStringKeySerializer());
        }

        RateLimiterMetadataDiff(StreamInput in) throws IOException {
            rateLimiters = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), RateLimiterConfiguration::readFrom,
                    RateLimiterConfiguration::readDiffFrom);
        }

        @Override
        public MetaData.Custom apply(MetaData.Custom part) {
            return new RateLimiterMetadata(rateLimiters.apply(((RateLimiterMetadata) part).rateLimiters));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            rateLimiters.writeTo(out);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }
    }
}
