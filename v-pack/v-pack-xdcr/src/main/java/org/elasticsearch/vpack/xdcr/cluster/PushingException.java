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

package org.elasticsearch.vpack.xdcr.cluster;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.vpack.xdcr.metadata.Feed;
import org.elasticsearch.vpack.xdcr.metadata.FeedId;

import java.io.IOException;

/**
 * Generic index exception
 */
public class PushingException extends ElasticsearchException {

    @Nullable
    private final String repositoryName;
    @Nullable
    private final String snapshotName;

    public PushingException(final Feed snapshot, final String msg) {
        this(snapshot, msg, null);
    }

    public PushingException(final Feed snapshot, final String msg, final Throwable cause) {
        super("[" + (snapshot == null ? "_na" : snapshot) + "] " + msg, cause);
        if (snapshot != null) {
            this.repositoryName = snapshot.getRepository();
            this.snapshotName = snapshot.getFeedId().getName();
        } else {
            this.repositoryName = null;
            this.snapshotName = null;
        }
    }

    public PushingException(final String repositoryName, final FeedId feedId, final String msg) {
        this(repositoryName, feedId, msg, null);
    }

    public PushingException(final String repositoryName, final FeedId feedId, final String msg, final Throwable cause) {
        super("[" + repositoryName + ":" + feedId + "] " + msg, cause);
        this.repositoryName = repositoryName;
        this.snapshotName = feedId.getName();
    }

    public PushingException(final String repositoryName, final String snapshotName, final String msg) {
        this(repositoryName, snapshotName, msg, null);
    }

    public PushingException(final String repositoryName, final String snapshotName, final String msg, final Throwable cause) {
        super("[" + repositoryName + ":" + snapshotName + "]" + msg, cause);
        this.repositoryName = repositoryName;
        this.snapshotName = snapshotName;
    }

    public PushingException(final StreamInput in) throws IOException {
        super(in);
        repositoryName = in.readOptionalString();
        snapshotName = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(repositoryName);
        out.writeOptionalString(snapshotName);
    }

}
