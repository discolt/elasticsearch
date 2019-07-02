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

package org.elasticsearch.vpack;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;

public class JoinSettings {

    /**
     * execute join thread queue size
     */
    public static final Setting<Integer> JOIN_THREAD_QUEUE_SIZE = Setting.intSetting("join.queue.size", 2, Property.Dynamic, Property.NodeScope);

    /**
     * terms query result byte size limit
     */
    public static final Setting<Integer> JOIN_TERMS_LIMIT = Setting.intSetting("join.terms.limit", 1000000, Property.Dynamic, Property.NodeScope);

}
