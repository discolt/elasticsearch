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

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;
import static org.elasticsearch.common.settings.Settings.writeSettingsToStream;
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
 * <li>must not contain invalid file getName characters {@link org.elasticsearch.common.Strings#INVALID_FILENAME_CHARS} </li>
 * </ul>
 */
public class CreateFeedRequest extends MasterNodeRequest<CreateFeedRequest> {

    private String indices;

    private String repository;

    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();

    private boolean partial = false;

    private Settings settings = EMPTY_SETTINGS;

    private boolean includeGlobalState = true;

    private boolean waitForCompletion;

    public CreateFeedRequest() {
    }


    /**
     * Constructs a new put repository request with the provided indices and repository names
     *
     * @param repository repository getName
     * @param indices    indices getName
     */
    public CreateFeedRequest(String repository, String indices) {
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
        if (indices == null) {
            validationException = addValidationError("indices is null", validationException);
        }
        if (indicesOptions == null) {
            validationException = addValidationError("indicesOptions is null", validationException);
        }
        if (settings == null) {
            validationException = addValidationError("settings is null", validationException);
        }
        return validationException;
    }

    /**
     * Sets the indices getName
     *
     * @param snapshot indices getName
     */
    public CreateFeedRequest index(String snapshot) {
        this.indices = snapshot;
        return this;
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
    public CreateFeedRequest repository(String repository) {
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

    /**
     * Specifies the indices options. Like what type of requested indices to ignore. For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices options
     * @return this request
     */
    public CreateFeedRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }


    /**
     * Returns true if indices with unavailable shards should be be partially snapshotted.
     *
     * @return the desired behaviour regarding indices options
     */
    public boolean partial() {
        return partial;
    }

    /**
     * Set to true to allow indices with unavailable shards to be partially snapshotted.
     *
     * @param partial true if indices with unavailable shards should be be partially snapshotted.
     * @return this request
     */
    public CreateFeedRequest partial(boolean partial) {
        this.partial = partial;
        return this;
    }

    /**
     * If set to true the operation should wait for the indices completion before returning.
     * <p>
     * By default, the operation will return as soon as indices is initialized. It can be changed by setting this
     * flag to true.
     *
     * @param waitForCompletion true if operation should wait for the indices completion
     * @return this request
     */
    public CreateFeedRequest waitForCompletion(boolean waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
        return this;
    }

    /**
     * Returns true if the request should wait for the indices completion before returning
     *
     * @return true if the request should wait for completion
     */
    public boolean waitForCompletion() {
        return waitForCompletion;
    }

    /**
     * Sets repository-specific indices settings.
     * <p>
     * See repository documentation for more information.
     *
     * @param settings repository-specific indices settings
     * @return this request
     */
    public CreateFeedRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * Sets repository-specific indices settings.
     * <p>
     * See repository documentation for more information.
     *
     * @param settings repository-specific indices settings
     * @return this request
     */
    public CreateFeedRequest settings(Settings.Builder settings) {
        this.settings = settings.build();
        return this;
    }

    /**
     * Sets repository-specific indices settings in JSON or YAML format
     * <p>
     * See repository documentation for more information.
     *
     * @param source       repository-specific indices settings
     * @param xContentType the content type of the source
     * @return this request
     */
    public CreateFeedRequest settings(String source, XContentType xContentType) {
        this.settings = Settings.builder().loadFromSource(source, xContentType).build();
        return this;
    }

    /**
     * Sets repository-specific indices settings.
     * <p>
     * See repository documentation for more information.
     *
     * @param source repository-specific indices settings
     * @return this request
     */
    public CreateFeedRequest settings(Map<String, Object> source) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.map(source);
            settings(Strings.toString(builder), builder.contentType());
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
        return this;
    }

    /**
     * Returns repository-specific indices settings
     *
     * @return repository-specific indices settings
     */
    public Settings settings() {
        return this.settings;
    }

    /**
     * Set to true if global state should be stored as part of the indices
     *
     * @param includeGlobalState true if global state should be stored
     * @return this request
     */
    public CreateFeedRequest includeGlobalState(boolean includeGlobalState) {
        this.includeGlobalState = includeGlobalState;
        return this;
    }

    /**
     * Returns true if global state should be stored as part of the indices
     *
     * @return true if global state should be stored as part of the indices
     */
    public boolean includeGlobalState() {
        return includeGlobalState;
    }

    /**
     * Parses indices definition.
     *
     * @param source indices definition
     * @return this request
     */
    @SuppressWarnings("unchecked")
    public CreateFeedRequest source(Map<String, Object> source) {
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            String name = entry.getKey();
            if (name.equals("partial")) {
                partial(nodeBooleanValue(entry.getValue(), "partial"));
            } else if (name.equals("settings")) {
                if (!(entry.getValue() instanceof Map)) {
                    throw new IllegalArgumentException("malformed settings section, should indices an inner object");
                }
                settings((Map<String, Object>) entry.getValue());
            } else if (name.equals("include_global_state")) {
                includeGlobalState = nodeBooleanValue(entry.getValue(), "include_global_state");
            }
        }
        indicesOptions(IndicesOptions.fromMap(source, indicesOptions));
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        indices = in.readString();
        repository = in.readString();
        indicesOptions = IndicesOptions.readIndicesOptions(in);
        settings = readSettingsFromStream(in);
        includeGlobalState = in.readBoolean();
        waitForCompletion = in.readBoolean();
        partial = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indices);
        out.writeString(repository);
        indicesOptions.writeIndicesOptions(out);
        writeSettingsToStream(settings, out);
        out.writeBoolean(includeGlobalState);
        out.writeBoolean(waitForCompletion);
        out.writeBoolean(partial);
    }

    @Override
    public String getDescription() {
        return "feed [" + repository + ":" + indices + "]";
    }

}
