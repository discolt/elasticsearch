package org.elasticsearch.vpack.xdcr.action.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class Stats implements Writeable, ToXContentFragment {

    public final String index;
    public final String repository;
    public final int shard;
    public final long localSeqno;
    public final long remoteSeqno;

    public Stats(String index, String repository, int shard, long localSeqno, long remoteSeqno) {
        this.index = index;
        this.repository = repository;
        this.shard = shard;
        this.localSeqno = localSeqno;
        this.remoteSeqno = remoteSeqno;
    }

    /**
     * Read from a stream.
     */
    public Stats(StreamInput in) throws IOException {
        index = in.readString();
        repository = in.readString();
        shard = in.readInt();
        localSeqno = in.readLong();
        remoteSeqno = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(repository);
        out.writeString(index);
        out.writeInt(shard);
        out.writeLong(localSeqno);
        out.writeLong(remoteSeqno);
    }

    public static Stats readStats(StreamInput in) throws IOException {
        return new Stats(in);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("index", index);
        builder.field("repository", repository);
        builder.field("shard", shard);
        builder.field("localSeqno", localSeqno);
        builder.field("remoteSeqno", remoteSeqno);
        builder.endObject();
        return builder;
    }
}