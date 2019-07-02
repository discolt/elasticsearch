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

package org.elasticsearch.vpack.limit.action.put;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

public class PutRateLimiterRequest extends AcknowledgedRequest<PutRateLimiterRequest> implements ToXContentObject {

    private String tenant;
    private BytesReference source;
    private XContentType xContentType;

    @Deprecated
    public PutRateLimiterRequest(String tenant, BytesReference source) {
        this(tenant, source, XContentHelper.xContentType(source));
    }

    public PutRateLimiterRequest(String tenant, BytesReference source, XContentType xContentType) {
        this.tenant = Objects.requireNonNull(tenant);
        this.source = Objects.requireNonNull(source);
        this.xContentType = Objects.requireNonNull(xContentType);
    }

    PutRateLimiterRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getTenant() {
        return tenant;
    }

    public BytesReference getSource() {
        return source;
    }

    public XContentType getXContentType() {
        return xContentType;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        tenant = in.readString();
        source = in.readBytesReference();
        if (in.getVersion().onOrAfter(Version.V_5_3_0)) {
            xContentType = in.readEnum(XContentType.class);
        } else {
            xContentType = XContentHelper.xContentType(source);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(tenant);
        out.writeBytesReference(source);
        if (out.getVersion().onOrAfter(Version.V_5_3_0)) {
            out.writeEnum(xContentType);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (source != null) {
            builder.rawValue(source.streamInput(), xContentType);
        } else {
            builder.startObject().endObject();
        }
        return builder;
    }
}
