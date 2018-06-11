/**
 * Copyright (c) 2016, SIREn Solutions. All Rights Reserved.
 * <p>
 * This file is part of the SIREn project.
 * <p>
 * SIREn is a free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 * <p>
 * SIREn is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public
 * License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.elasticsearch.search.join.terms;

import org.elasticsearch.action.support.broadcast.BroadcastShardRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;

import java.io.IOException;
import java.util.Objects;

/**
 * Internal terms by query request executed directly against a specific index shard.
 */
public class TermsFetchShardRequest extends BroadcastShardRequest {

    private TermsFetchRequest request;
    private String[] types = Strings.EMPTY_ARRAY;
    private String field;
    private SearchSourceBuilder source;
    private long nowInMillis;
    private AliasFilter filteringAliases;

    /**
     * Default constructor
     */
    public TermsFetchShardRequest() {
    }


    public TermsFetchShardRequest(ShardId shardId, AliasFilter filteringAliases, TermsFetchRequest request) {
        super(shardId, request);
        this.types = request.types();
        this.field = request.field();
        this.source = request.source();
        this.nowInMillis = request.nowInMillis();
        this.filteringAliases = Objects.requireNonNull(filteringAliases, "filteringAliases must not be null");
    }

    public TermsFetchRequest request() {
        return request;
    }

    public String field() {
        return field;
    }

    public String[] types() {
        return types;
    }

    public SearchSourceBuilder source() {
        return source;
    }

    public long nowInMillis() {
        return nowInMillis;
    }

    public AliasFilter filteringAliases() {
        return filteringAliases;
    }

    /**
     * Deserialize
     */
    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        request = new TermsFetchRequest();
        request.readFrom(in);

        if (in.readBoolean()) {
            filteringAliases = new AliasFilter(in);
        }
    }

    /**
     * Serialize
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);

        if (filteringAliases == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            filteringAliases.writeTo(out);
        }
    }

}