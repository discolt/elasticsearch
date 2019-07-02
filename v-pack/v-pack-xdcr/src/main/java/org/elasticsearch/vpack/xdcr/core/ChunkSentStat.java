package org.elasticsearch.vpack.xdcr.core;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

public class ChunkSentStat implements Streamable, Writeable, ToXContentFragment {

    private String repository;
    private ShardId shardId;
    private long localCheckpoint;
    private long remoteCheckpoint;
    private String status;
    private String errors;


    public ChunkSentStat() {

    }

    public ChunkSentStat(String repository, ShardId shardId, long localCheckpoint, long remoteCheckpoint, String status, String errors) {
        this.repository = repository;
        this.shardId = shardId;
        this.localCheckpoint = localCheckpoint;
        this.remoteCheckpoint = remoteCheckpoint;
        this.status = status;
        this.errors = errors;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        repository = in.readString();
        shardId = ShardId.readShardId(in);
        localCheckpoint = in.readLong();
        remoteCheckpoint = in.readLong();
        status = in.readString();
        errors = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(repository);
        shardId.writeTo(out);
        out.writeLong(localCheckpoint);
        out.writeLong(remoteCheckpoint);
        out.writeString(status);
        out.writeString(errors);
    }

    public static ChunkSentStat read(final StreamInput in) throws IOException {
        ChunkSentStat rec = new ChunkSentStat();
        rec.repository = in.readString();
        rec.shardId = ShardId.readShardId(in);
        rec.localCheckpoint = in.readLong();
        rec.remoteCheckpoint = in.readLong();
        rec.status = in.readString();
        rec.errors = in.readString();
        return rec;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(repository + ":" + shardId.toString());
        builder.field("status", status);
        builder.field("checkpoint(leader)", localCheckpoint);
        builder.field("checkpoint(follower)", remoteCheckpoint);
        if (errors != null && errors.trim().length() > 0) {
            builder.field("errors", errors);
        }
        builder.endObject();
        return builder;
    }
}
