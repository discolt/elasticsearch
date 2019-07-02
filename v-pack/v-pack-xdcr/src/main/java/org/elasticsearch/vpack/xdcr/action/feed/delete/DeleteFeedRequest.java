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

package org.elasticsearch.vpack.xdcr.action.feed.delete;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeBooleanValue;

/**
 * Create indices request
 * <p>
 * The only mandatory parameter is repository getName. The repository getName has to satisfy the following requirements
 * <ul>
 * <li>be a non-empty string</li>
 * <li>must not contain whitespace (tabs or spaces)</li>
 * <li>must not contain comma (',')</li>
 * <li>must not contain hash sign ('#')</li>
 * <li>must not start with underscore ('-')</li>
 * <li>must be lowercase</li>
 * <li>must not contain invalid file getName characters {@link Strings#INVALID_FILENAME_CHARS} </li>
 * </ul>
 */
public class DeleteFeedRequest extends MasterNodeRequest<DeleteFeedRequest> {

    private String indices;

    private String repository;

    public DeleteFeedRequest() {
    }

    /**
     * Constructs a new put repository request with the provided indices and repository names
     *
     * @param repository repository getName
     * @param indices    indices getName
     */
    public DeleteFeedRequest(String repository, String indices) {
        this.indices = indices;
        this.repository = repository;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indices == null) {
            validationException = addValidationError("indices is missing", validationException);
        }
        if (repository == null) {
            validationException = addValidationError("repository is missing", validationException);
        }
        return validationException;
    }

    /**
     * The indices getName
     *
     * @return indices getName
     */
    public String indices() {
        return this.indices;
    }

    /**
     * Sets repository getName
     *
     * @param repository getName
     * @return this request
     */
    public DeleteFeedRequest repository(String repository) {
        this.repository = repository;
        return this;
    }

    /**
     * Returns repository getName
     *
     * @return repository getName
     */
    public String repository() {
        return this.repository;
    }



    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        indices = in.readString();
        repository = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indices);
        out.writeString(repository);
    }

    @Override
    public String getDescription() {
        return "delete feed [" + repository + ":" + indices + "]";
    }

}
