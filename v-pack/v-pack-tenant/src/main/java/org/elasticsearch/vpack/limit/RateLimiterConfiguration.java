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

package org.elasticsearch.vpack.limit;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.*;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public final class RateLimiterConfiguration extends AbstractDiffable<RateLimiterConfiguration> implements ToXContentObject {

    private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>("limit_conf", Builder::new);
    static {
        PARSER.declareString(Builder::setTenant, new ParseField("tenant"));
        PARSER.declareField((parser, builder, aVoid) -> {
            XContentBuilder contentBuilder = XContentBuilder.builder(parser.contentType().xContent());
            contentBuilder.generator().copyCurrentStructure(parser);
            builder.setConfig(BytesReference.bytes(contentBuilder), contentBuilder.contentType());
        }, new ParseField("config"), ObjectParser.ValueType.OBJECT);

    }

    public static ContextParser<Void, RateLimiterConfiguration> getParser() {
        return (parser, context) -> PARSER.apply(parser, null).build();
    }
    private static class Builder {

        private String tenant;
        private BytesReference config;
        private XContentType xContentType;

        void setTenant(String tenant) {
            this.tenant = tenant;
        }

        void setConfig(BytesReference config, XContentType xContentType) {
            this.config = config;
            this.xContentType = xContentType;
        }

        RateLimiterConfiguration build() {
            return new RateLimiterConfiguration(tenant, config, xContentType);
        }
    }

    private final String tenant;
    private final BytesReference config;
    private final XContentType xContentType;

    public RateLimiterConfiguration(String tenant, BytesReference config, XContentType xContentType) {
        this.tenant = Objects.requireNonNull(tenant);
        this.config = Objects.requireNonNull(config);
        this.xContentType = Objects.requireNonNull(xContentType);
    }

    public String getTenant() {
        return tenant;
    }

    public Map<String, Object> getConfigAsMap() {
        return XContentHelper.convertToMap(config, true, xContentType).v2();
    }

    XContentType getXContentType() {
        return xContentType;
    }

    BytesReference getConfig() {
        return config;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("tenant", tenant);
        builder.field("config", getConfigAsMap());
        builder.endObject();
        return builder;
    }

    public static RateLimiterConfiguration readFrom(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_5_3_0)) {
            return new RateLimiterConfiguration(in.readString(), in.readBytesReference(), in.readEnum(XContentType.class));
        } else {
            final String tenant = in.readString();
            final BytesReference config = in.readBytesReference();
            return new RateLimiterConfiguration(tenant, config, XContentHelper.xContentType(config));
        }
    }

    public static Diff<RateLimiterConfiguration> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(RateLimiterConfiguration::readFrom, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(tenant);
        out.writeBytesReference(config);
        if (out.getVersion().onOrAfter(Version.V_5_3_0)) {
            out.writeEnum(xContentType);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RateLimiterConfiguration that = (RateLimiterConfiguration) o;

        if (!tenant.equals(that.tenant)) return false;
        return getConfigAsMap().equals(that.getConfigAsMap());

    }

    @Override
    public int hashCode() {
        int result = tenant.hashCode();
        result = 31 * result + getConfigAsMap().hashCode();
        return result;
    }
}
