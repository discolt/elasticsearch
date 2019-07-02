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

import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Create index request builder
 */
public class DeleteFeedRequestBuilder extends MasterNodeOperationRequestBuilder<DeleteFeedRequest, DeleteFeedResponse, DeleteFeedRequestBuilder> {

    /**
     * Constructs a new createOrUpdate index request builder
     */
    public DeleteFeedRequestBuilder(ElasticsearchClient client, DeleteFeedAction action) {
        super(client, action, new DeleteFeedRequest());
    }

    /**
     * Constructs a new createOrUpdate index request builder with specified repository and index names
     */
    public DeleteFeedRequestBuilder(ElasticsearchClient client, DeleteFeedAction action, String repository, String snapshot) {
        super(client, action, new DeleteFeedRequest(repository, snapshot));
    }

    /**
     * Sets the repository getName
     *
     * @param repository repository getName
     * @return this builder
     */
    public DeleteFeedRequestBuilder setRepository(String repository) {
        request.repository(repository);
        return this;
    }

}
