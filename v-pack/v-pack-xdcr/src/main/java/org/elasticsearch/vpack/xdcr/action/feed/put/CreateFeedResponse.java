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

package org.elasticsearch.vpack.xdcr.action.feed.put;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.vpack.xdcr.metadata.FeedInfo;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * Create index response
 */
public class CreateFeedResponse extends ActionResponse implements ToXContentObject {

    @Nullable
    private FeedInfo feedInfo;

    CreateFeedResponse(@Nullable FeedInfo feedInfo) {
        this.feedInfo = feedInfo;
    }

    CreateFeedResponse() {
    }

    /**
     * Returns index information if index was completed by the time this method returned or null otherwise.
     *
     * @return index information or null
     */
    public FeedInfo getFeedInfo() {
        return feedInfo;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        feedInfo = in.readOptionalWriteable(FeedInfo::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(feedInfo);
    }

    /**
     * Returns HTTP status
     * <ul>
     * <li>{@link RestStatus#ACCEPTED} if index is still in progress</li>
     * <li>{@link RestStatus#OK} if index was successful or partially successful</li>
     * <li>{@link RestStatus#INTERNAL_SERVER_ERROR} if index failed completely</li>
     * </ul>
     */
    public RestStatus status() {
        if (feedInfo == null) {
            return RestStatus.ACCEPTED;
        }
        return feedInfo.status();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (feedInfo != null) {
            builder.field("index");
            feedInfo.toXContent(builder, params);
        } else {
            builder.field("accepted", true);
        }
        builder.endObject();
        return builder;
    }
}
