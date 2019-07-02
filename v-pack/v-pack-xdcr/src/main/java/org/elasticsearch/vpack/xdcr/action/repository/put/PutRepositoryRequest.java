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

package org.elasticsearch.vpack.xdcr.action.repository.put;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
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

/**
 * Register repository request.
 * <p>
 * Registers a repository with given getName, type and settings. If the repository with the same getName already
 * exists in the cluster, the new repository will replace the existing repository.
 */
public class PutRepositoryRequest extends AcknowledgedRequest<PutRepositoryRequest> {

    private String name;

    private boolean verify = true;

    private Settings settings = EMPTY_SETTINGS;

    public PutRepositoryRequest() {
    }

    /**
     * Constructs a new put repository request with the provided getName.
     */
    public PutRepositoryRequest(String name) {
        this.name = name;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (name == null) {
            validationException = addValidationError("getName is missing", validationException);
        }
        return validationException;
    }

    /**
     * Sets the getName of the repository.
     *
     * @param name repository getName
     */
    public PutRepositoryRequest name(String name) {
        this.name = name;
        return this;
    }

    /**
     * The getName of the repository.
     *
     * @return repository getName
     */
    public String name() {
        return this.name;
    }

    /**
     * Sets the repository settings
     *
     * @param settings repository settings
     * @return this request
     */
    public PutRepositoryRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * Sets the repository settings
     *
     * @param settings repository settings
     * @return this request
     */
    public PutRepositoryRequest settings(Settings.Builder settings) {
        this.settings = settings.build();
        return this;
    }

    /**
     * Sets the repository settings.
     *
     * @param source       repository settings in json or yaml format
     * @param xContentType the content type of the source
     * @return this request
     */
    public PutRepositoryRequest settings(String source, XContentType xContentType) {
        this.settings = Settings.builder().loadFromSource(source, xContentType).build();
        return this;
    }

    /**
     * Sets the repository settings.
     *
     * @param source repository settings
     * @return this request
     */
    public PutRepositoryRequest settings(Map<String, Object> source) {
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
     * Returns repository settings
     *
     * @return repository settings
     */
    public Settings settings() {
        return this.settings;
    }

    /**
     * Sets whether or not the repository should be verified after creation
     */
    public PutRepositoryRequest verify(boolean verify) {
        this.verify = verify;
        return this;
    }

    /**
     * Returns true if repository should be verified after creation
     */
    public boolean verify() {
        return this.verify;
    }

    /**
     * Parses repository definition.
     *
     * @param repositoryDefinition repository definition
     */
    public PutRepositoryRequest source(Map<String, Object> repositoryDefinition) {
        for (Map.Entry<String, Object> entry : repositoryDefinition.entrySet()) {
            String name = entry.getKey();
            if (name.equals("settings")) {
                if (!(entry.getValue() instanceof Map)) {
                    throw new IllegalArgumentException("Malformed settings section, should include an inner object");
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> sub = (Map<String, Object>) entry.getValue();
                settings(sub);
            }
        }
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        name = in.readString();
        settings = readSettingsFromStream(in);
        verify = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        writeSettingsToStream(settings, out);
        out.writeBoolean(verify);
    }
}
