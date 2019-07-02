/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.vpack.xdcr.metadata;

import com.carrotsearch.hppc.ObjectContainer;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState.Custom;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.*;

/**
 * Meta data about feed that are currently executing
 */
public class FeedInProgress extends AbstractNamedDiffable<Custom> implements Custom {
    public static final String TYPE = "xdcr_pushing";

    // denotes an undefined repositories state id, which will happen when receiving a cluster state with
    // a index in progress from a pre 5.2.x node
    public static final long UNDEFINED_REPOSITORY_STATE_ID = -2L;
    // the version where repositories state ids were introduced
    private static final Version REPOSITORY_ID_INTRODUCED_VERSION = Version.V_5_2_0;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FeedInProgress that = (FeedInProgress) o;

        if (!entries.equals(that.entries)) return false;

        return true;
    }

    private final List<Entry> entries;

    public FeedInProgress(List<Entry> entries) {
        this.entries = entries;
    }

    public FeedInProgress(Entry... entries) {
        this.entries = Arrays.asList(entries);
    }

    public List<Entry> entries() {
        return this.entries;
    }

    public Entry feed(final Feed feed) {
        for (Entry entry : entries) {
            final Feed curr = entry.feed();
            if (curr.equals(feed)) {
                return entry;
            }
        }
        return null;
    }

    public boolean hasFeed(String feedKey) {
        if (entries.isEmpty()) {
            return false;
        }
        return entries.stream().anyMatch(p -> p.feed.key().equals(feedKey));
    }

    public boolean waitingShardExisted() {
        for (Entry entry : entries) {
            for (ObjectObjectCursor<ShardId, ShardPushingStatus> cursor : entry.shards) {
                if (cursor.value.state == State.MISSING || cursor.value.state == State.FAILED
                        || cursor.value.state == State.ABORTED || cursor.value.state == State.WAITING) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_6_3_0;
    }

    public static NamedDiff<Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Custom.class, TYPE, in);
    }

    public FeedInProgress(StreamInput in) throws IOException {
        Entry[] entries = new Entry[in.readVInt()];
        for (int i = 0; i < entries.length; i++) {
            Feed snapshot = new Feed(in);
            State state = State.fromValue(in.readByte());
            int indices = in.readVInt();
            List<IndexId> indexBuilder = new ArrayList<>();
            for (int j = 0; j < indices; j++) {
                indexBuilder.add(new IndexId(in.readString(), in.readString()));
            }
            long startTime = in.readLong();
            ImmutableOpenMap.Builder<ShardId, ShardPushingStatus> builder = ImmutableOpenMap.builder();
            int shards = in.readVInt();
            for (int j = 0; j < shards; j++) {
                ShardId shardId = ShardId.readShardId(in);
                if (in.getVersion().onOrAfter(Version.V_6_0_0_beta1)) {
                    builder.put(shardId, new ShardPushingStatus(in));
                } else {
                    String nodeId = in.readOptionalString();
                    State shardState = State.fromValue(in.readByte());
                    // Workaround for https://github.com/elastic/elasticsearch/issues/25878
                    // Some old index might still have null in shard failure reasons
                    String reason = shardState.failed() ? "" : null;
                    builder.put(shardId, new ShardPushingStatus(nodeId, shardState, reason));
                }
            }
            long repositoryStateId = UNDEFINED_REPOSITORY_STATE_ID;
            if (in.getVersion().onOrAfter(REPOSITORY_ID_INTRODUCED_VERSION)) {
                repositoryStateId = in.readLong();
            }
            entries[i] = new Entry(snapshot,
                    state,
                    Collections.unmodifiableList(indexBuilder),
                    startTime,
                    repositoryStateId,
                    builder.build());
        }
        this.entries = Arrays.asList(entries);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(entries.size());
        for (Entry entry : entries) {
            entry.feed().writeTo(out);
            out.writeByte(entry.state().value());
            out.writeVInt(entry.indices().size());
            for (IndexId index : entry.indices()) {
                index.writeTo(out);
            }
            out.writeLong(entry.startTime());
            out.writeVInt(entry.shards().size());
            for (ObjectObjectCursor<ShardId, ShardPushingStatus> shardEntry : entry.shards()) {
                shardEntry.key.writeTo(out);
                // TODO: Change this to an appropriate version when it's backported
                if (out.getVersion().onOrAfter(Version.V_6_0_0_beta1)) {
                    shardEntry.value.writeTo(out);
                } else {
                    out.writeOptionalString(shardEntry.value.nodeId());
                    out.writeByte(shardEntry.value.state().value());
                }
            }
            if (out.getVersion().onOrAfter(REPOSITORY_ID_INTRODUCED_VERSION)) {
                out.writeLong(entry.repositoryStateId);
            }
        }
    }

    private static final String REPOSITORY = "repositories";
    private static final String FEEDS = "feeds";
    private static final String FEED = "feed";
    private static final String UUID = "uuid";
    private static final String STATE = "state";
    private static final String INDICES = "indices";
    private static final String START_TIME_MILLIS = "start_time_millis";
    private static final String START_TIME = "start_time";
    private static final String REPOSITORY_STATE_ID = "repository_state_id";
    private static final String SHARDS = "shards";
    private static final String INDEX = "index";
    private static final String SHARD = "shard";
    private static final String NODE = "node";

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startArray(FEEDS);
        for (Entry entry : entries) {
            toXContent(entry, builder, params);
        }
        builder.endArray();
        return builder;
    }

    public void toXContent(Entry entry, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field(REPOSITORY, entry.feed().getRepository());
        builder.field(FEED, entry.feed().getFeedId().getName());
        builder.field(UUID, entry.feed().getFeedId().getUUID());
        builder.field(STATE, entry.state());
        builder.startArray(INDICES);
        {
            for (IndexId index : entry.indices()) {
                index.toXContent(builder, params);
            }
        }
        builder.endArray();
        builder.timeField(START_TIME_MILLIS, START_TIME, entry.startTime());
        builder.field(REPOSITORY_STATE_ID, entry.getRepositoryStateId());
        builder.startArray(SHARDS);
        {
            for (ObjectObjectCursor<ShardId, ShardPushingStatus> shardEntry : entry.shards) {
                ShardId shardId = shardEntry.key;
                ShardPushingStatus status = shardEntry.value;
                builder.startObject();
                {
                    builder.field(INDEX, shardId.getIndex());
                    builder.field(SHARD, shardId.getId());
                    builder.field(STATE, status.state());
                    builder.field(NODE, status.nodeId());
                }
                builder.endObject();
            }
        }
        builder.endArray();
        builder.endObject();
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("TasksInProgress[");
        for (int i = 0; i < entries.size(); i++) {
            builder.append(entries.get(i).feed().getFeedId().getName());
            if (i + 1 < entries.size()) {
                builder.append(",");
            }
        }
        return builder.append("]").toString();
    }

    public static class Entry {
        private final State state;
        private final Feed feed;
        private final ImmutableOpenMap<ShardId, ShardPushingStatus> shards;
        private final List<IndexId> indices;
        private final ImmutableOpenMap<String, List<ShardId>> waitingIndices;
        private final long startTime;
        private final long repositoryStateId;

        public Entry(Feed feed, State state, List<IndexId> indices,
                     long startTime, long repositoryStateId, ImmutableOpenMap<ShardId, ShardPushingStatus> shards) {
            this.state = state;
            this.feed = feed;
            this.indices = indices;
            this.startTime = startTime;
            if (shards == null) {
                this.shards = ImmutableOpenMap.of();
                this.waitingIndices = ImmutableOpenMap.of();
            } else {
                this.shards = shards;
                this.waitingIndices = findWaitingIndices(shards);
            }
            this.repositoryStateId = repositoryStateId;
        }

        public Entry(Entry entry, State state, ImmutableOpenMap<ShardId, ShardPushingStatus> shards) {
            this(entry.feed, state, entry.indices, entry.startTime,
                    entry.repositoryStateId, shards);
        }

        public Entry(Entry entry, ImmutableOpenMap<ShardId, ShardPushingStatus> shards) {
            this(entry, entry.state, shards);
        }

        public Feed feed() {
            return this.feed;
        }

        public ImmutableOpenMap<ShardId, ShardPushingStatus> shards() {
            return this.shards;
        }

        public ShardPushingStatus shard(ShardId shardId) {
            return this.shards.get(shardId);
        }

        public State state() {
            return state;
        }

        public List<IndexId> indices() {
            return indices;
        }

        public ImmutableOpenMap<String, List<ShardId>> waitingIndices() {
            return waitingIndices;
        }

        public long startTime() {
            return startTime;
        }

        public long getRepositoryStateId() {
            return repositoryStateId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Entry entry = (Entry) o;
            if (startTime != entry.startTime) return false;
            if (!indices.equals(entry.indices)) return false;
            if (!shards.equals(entry.shards)) return false;
            if (!feed.equals(entry.feed)) return false;
            if (state != entry.state) return false;
            if (!waitingIndices.equals(entry.waitingIndices)) return false;
            if (repositoryStateId != entry.repositoryStateId) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = state.hashCode();
            result = 31 * result + feed.hashCode();
            result = 31 * result + shards.hashCode();
            result = 31 * result + indices.hashCode();
            result = 31 * result + waitingIndices.hashCode();
            result = 31 * result + Long.hashCode(startTime);
            result = 31 * result + Long.hashCode(repositoryStateId);
            return result;
        }

        @Override
        public String toString() {
            return feed.toString();
        }

        // package private for testing
        ImmutableOpenMap<String, List<ShardId>> findWaitingIndices(ImmutableOpenMap<ShardId, ShardPushingStatus> shards) {
            Map<String, List<ShardId>> waitingIndicesMap = new HashMap<>();
            for (ObjectObjectCursor<ShardId, ShardPushingStatus> entry : shards) {
                if (entry.value.state() == State.WAITING) {
                    final String indexName = entry.key.getIndexName();
                    List<ShardId> waitingShards = waitingIndicesMap.get(indexName);
                    if (waitingShards == null) {
                        waitingShards = new ArrayList<>();
                        waitingIndicesMap.put(indexName, waitingShards);
                    }
                    waitingShards.add(entry.key);
                }
            }
            if (waitingIndicesMap.isEmpty()) {
                return ImmutableOpenMap.of();
            }
            ImmutableOpenMap.Builder<String, List<ShardId>> waitingIndicesBuilder = ImmutableOpenMap.builder();
            for (Map.Entry<String, List<ShardId>> entry : waitingIndicesMap.entrySet()) {
                waitingIndicesBuilder.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));
            }
            return waitingIndicesBuilder.build();
        }
    }

    /**
     * Checks if all shards in the list have completed
     *
     * @param shards list of shard statuses
     * @return true if all shards have completed (either successfully or failed), false otherwise
     */
    public static boolean completed(ObjectContainer<ShardPushingStatus> shards) {
        for (ObjectCursor<ShardPushingStatus> status : shards) {
            if (status.value.state().completed() == false) {
                return false;
            }
        }
        return true;
    }


    public static class ShardPushingStatus {
        private final State state;
        private final String nodeId;
        private final String reason;

        public ShardPushingStatus(String nodeId) {
            this(nodeId, State.INIT);
        }

        public ShardPushingStatus(String nodeId, State state) {
            this(nodeId, state, null);
        }

        public ShardPushingStatus(String nodeId, State state, String reason) {
            this.nodeId = nodeId;
            this.state = state;
            this.reason = reason;
            // If the state is failed we have to have a reason for this failure
            assert state.failed() == false || reason != null;
        }

        public ShardPushingStatus(StreamInput in) throws IOException {
            nodeId = in.readOptionalString();
            state = State.fromValue(in.readByte());
            reason = in.readOptionalString();
        }

        public State state() {
            return state;
        }

        public String nodeId() {
            return nodeId;
        }

        public String reason() {
            return reason;
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(nodeId);
            out.writeByte(state.value);
            out.writeOptionalString(reason);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ShardPushingStatus status = (ShardPushingStatus) o;

            if (nodeId != null ? !nodeId.equals(status.nodeId) : status.nodeId != null) return false;
            if (reason != null ? !reason.equals(status.reason) : status.reason != null) return false;
            if (state != status.state) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = state != null ? state.hashCode() : 0;
            result = 31 * result + (nodeId != null ? nodeId.hashCode() : 0);
            result = 31 * result + (reason != null ? reason.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return state.toString();
        }
    }

    public enum State {
        INIT((byte) 0, false, false),
        STARTED((byte) 1, false, false),
        SUCCESS((byte) 2, true, false),
        FAILED((byte) 3, true, true),
        ABORTED((byte) 4, false, true),
        MISSING((byte) 5, true, true),
        WAITING((byte) 6, false, false);

        private byte value;

        private boolean completed;

        private boolean failed;

        State(byte value, boolean completed, boolean failed) {
            this.value = value;
            this.completed = completed;
            this.failed = failed;
        }

        public byte value() {
            return value;
        }

        public boolean completed() {
            return completed;
        }

        public boolean failed() {
            return failed;
        }

        public static State fromValue(byte value) {
            switch (value) {
                case 0:
                    return INIT;
                case 1:
                    return STARTED;
                case 2:
                    return SUCCESS;
                case 3:
                    return FAILED;
                case 4:
                    return ABORTED;
                case 5:
                    return MISSING;
                case 6:
                    return WAITING;
                default:
                    throw new IllegalArgumentException("No index state for value [" + value + "]");
            }
        }
    }
}
