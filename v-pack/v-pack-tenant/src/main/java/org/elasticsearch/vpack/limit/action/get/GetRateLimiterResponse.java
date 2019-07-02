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

package org.elasticsearch.vpack.limit.action.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.vpack.limit.RateLimiterConfiguration;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class GetRateLimiterResponse extends ActionResponse implements StatusToXContentObject {

    private List<RateLimiterConfiguration> limiters;

    public GetRateLimiterResponse() {
    }

    public GetRateLimiterResponse(List<RateLimiterConfiguration> limiters) {
        this.limiters = limiters;
    }

    public List<RateLimiterConfiguration> limiters() {
        return Collections.unmodifiableList(limiters);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        limiters = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            limiters.add(RateLimiterConfiguration.readFrom(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(limiters.size());
        for (RateLimiterConfiguration pipeline : limiters) {
            pipeline.writeTo(out);
        }
    }

    public boolean isFound() {
        return !limiters.isEmpty();
    }

    @Override
    public RestStatus status() {
        return isFound() ? RestStatus.OK : RestStatus.NOT_FOUND;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (RateLimiterConfiguration limiter : limiters) {
            builder.field(limiter.getTenant(), limiter.getConfigAsMap());
        }
        builder.endObject();
        return builder;
    }

    public static GetRateLimiterResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
        List<RateLimiterConfiguration> pipelines = new ArrayList<>();
        while(parser.nextToken().equals(Token.FIELD_NAME)) {
            String limiterId = parser.currentName();
            parser.nextToken();
            XContentBuilder contentBuilder = XContentBuilder.builder(parser.contentType().xContent());
            contentBuilder.generator().copyCurrentStructure(parser);
            RateLimiterConfiguration pipeline =
                new RateLimiterConfiguration(
                    limiterId, BytesReference.bytes(contentBuilder), contentBuilder.contentType()
                );
            pipelines.add(pipeline);
        }
        ensureExpectedToken(Token.END_OBJECT, parser.currentToken(), parser::getTokenLocation);
        return new GetRateLimiterResponse(pipelines);
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        } else if (other instanceof GetRateLimiterResponse){
            GetRateLimiterResponse otherResponse = (GetRateLimiterResponse)other;
            if (limiters == null) {
                return otherResponse.limiters == null;
            } else {
                Map<String, RateLimiterConfiguration> otherLimiterMap = new HashMap<>();
                for (RateLimiterConfiguration limiter: otherResponse.limiters) {
                    otherLimiterMap.put(limiter.getTenant(), limiter);
                }
                for (RateLimiterConfiguration limiter: limiters) {
                    RateLimiterConfiguration otherLimiter = otherLimiterMap.get(limiter.getTenant());
                    if (!limiter.equals(otherLimiter)) {
                        return false;
                    }
                }
                return true;
            }
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int result = 1;
        for (RateLimiterConfiguration limiter: limiters) {
            // We only take the sum here to ensure that the order does not matter.
            result += (limiter == null ? 0 : limiter.hashCode());
        }
        return result;
    }

}
