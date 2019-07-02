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

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Map;

/**
 * Create index request builder
 */
public class CreateFeedRequestBuilder extends MasterNodeOperationRequestBuilder<CreateFeedRequest, CreateFeedResponse, CreateFeedRequestBuilder> {

    /**
     * Constructs a new createOrUpdate index request builder
     */
    public CreateFeedRequestBuilder(ElasticsearchClient client, CreateFeedAction action) {
        super(client, action, new CreateFeedRequest());
    }

    /**
     * Constructs a new createOrUpdate index request builder with specified repository and index names
     */
    public CreateFeedRequestBuilder(ElasticsearchClient client, CreateFeedAction action, String repository, String snapshot) {
        super(client, action, new CreateFeedRequest(repository, snapshot));
    }

    /**
     * Sets the index getName
     *
     * @param snapshot index getName
     * @return this builder
     */
    public CreateFeedRequestBuilder setSnapshot(String snapshot) {
        request.index(snapshot);
        return this;
    }

    /**
     * Sets the repository getName
     *
     * @param repository repository getName
     * @return this builder
     */
    public CreateFeedRequestBuilder setRepository(String repository) {
        request.repository(repository);
        return this;
    }


    /**
     * Specifies the indices options. Like what type of requested indices to ignore. For example indices that don't exist.
     *
     * @param indicesOptions the desired behaviour regarding indices options
     * @return this request
     */
    public CreateFeedRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }

    /**
     * If set to true the request should wait for the index completion before returning.
     *
     * @param waitForCompletion true if
     * @return this builder
     */
    public CreateFeedRequestBuilder setWaitForCompletion(boolean waitForCompletion) {
        request.waitForCompletion(waitForCompletion);
        return this;
    }

    /**
     * If set to true the request should index indices with unavailable shards
     *
     * @param partial true if request should index indices with unavailable shards
     * @return this builder
     */
    public CreateFeedRequestBuilder setPartial(boolean partial) {
        request.partial(partial);
        return this;
    }

    /**
     * Sets repository-specific index settings.
     * <p>
     * See repository documentation for more information.
     *
     * @param settings repository-specific index settings
     * @return this builder
     */
    public CreateFeedRequestBuilder setSettings(Settings settings) {
        request.settings(settings);
        return this;
    }

    /**
     * Sets repository-specific index settings.
     * <p>
     * See repository documentation for more information.
     *
     * @param settings repository-specific index settings
     * @return this builder
     */
    public CreateFeedRequestBuilder setSettings(Settings.Builder settings) {
        request.settings(settings);
        return this;
    }

    /**
     * Sets repository-specific index settings in YAML or JSON format
     * <p>
     * See repository documentation for more information.
     *
     * @param source repository-specific index settings
     * @param xContentType the content type of the source
     * @return this builder
     */
    public CreateFeedRequestBuilder setSettings(String source, XContentType xContentType) {
        request.settings(source, xContentType);
        return this;
    }

    /**
     * Sets repository-specific index settings.
     * <p>
     * See repository documentation for more information.
     *
     * @param settings repository-specific index settings
     * @return this builder
     */
    public CreateFeedRequestBuilder setSettings(Map<String, Object> settings) {
        request.settings(settings);
        return this;
    }

    /**
     * Set to true if index should include global cluster state
     *
     * @param includeGlobalState true if index should include global cluster state
     * @return this builder
     */
    public CreateFeedRequestBuilder setIncludeGlobalState(boolean includeGlobalState) {
        request.includeGlobalState(includeGlobalState);
        return this;
    }
}
