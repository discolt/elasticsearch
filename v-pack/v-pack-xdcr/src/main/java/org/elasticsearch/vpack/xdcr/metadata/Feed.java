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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * Basic information about a index - a SnapshotId and the repositories that the index belongs to.
 */
public final class Feed implements Writeable {

    private final String repository;
    private final FeedId feedId;
    private final int hashCode;

    /**
     * Constructs a index.
     */
    public Feed(final String repository, final FeedId feedId) {
        this.repository = Objects.requireNonNull(repository);
        this.feedId = Objects.requireNonNull(feedId);
        this.hashCode = computeHashCode();
    }

    /**
     * Constructs a index from the stream input.
     */
    public Feed(final StreamInput in) throws IOException {
        repository = in.readString();
        feedId = new FeedId(in);
        hashCode = computeHashCode();
    }

    /**
     * Gets the repositories getName for the index.
     */
    public String getRepository() {
        return repository;
    }

    /**
     * Gets the index id for the index.
     */
    public FeedId getFeedId() {
        return feedId;
    }

    public String key() {
        return repository + ":" + feedId.getName();
    }

    public static String getKey(String repository, String indices) {
        return repository + ":" + indices;
    }

    @Override
    public String toString() {
        return repository + ":" + feedId.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        @SuppressWarnings("unchecked") Feed that = (Feed) o;
        return repository.equals(that.repository) && feedId.equals(that.feedId);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private int computeHashCode() {
        return Objects.hash(repository, feedId);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(repository);
        feedId.writeTo(out);
    }

}
