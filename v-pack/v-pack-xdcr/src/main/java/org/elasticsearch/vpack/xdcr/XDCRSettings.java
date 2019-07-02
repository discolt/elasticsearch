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

package org.elasticsearch.vpack.xdcr;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

public class XDCRSettings extends AbstractComponent {

    /**
     * translog chunk size 一次传输的数据大小
     */
    public static final Setting<ByteSizeValue> RECOVERY_CHUNK_SIZE =
            Setting.byteSizeSetting("xdcr.chunk_size", new ByteSizeValue(20, ByteSizeUnit.MB),
                    Property.Dynamic, Property.NodeScope);

    /**
     * 不存在新数据时的等待时间
     */
    public static final Setting<TimeValue> RETRY_DEFAULT_TIME =
            Setting.positiveTimeSetting("xdcr.retry.default.time", TimeValue.timeValueSeconds(3),
                    Property.Dynamic, Property.NodeScope);


    private volatile ByteSizeValue chunkSize;
    private volatile TimeValue retryDefaultTime;

    public XDCRSettings(Settings settings, ClusterSettings clusterSettings) {
        super(settings);

        chunkSize = RECOVERY_CHUNK_SIZE.get(settings);
        retryDefaultTime = RETRY_DEFAULT_TIME.get(settings);

        clusterSettings.addSettingsUpdateConsumer(RECOVERY_CHUNK_SIZE, this::setChunkSize);
        clusterSettings.addSettingsUpdateConsumer(RETRY_DEFAULT_TIME, this::setRetryDefaultTime);
    }

    public ByteSizeValue getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(ByteSizeValue chunkSize) {
        if (chunkSize.bytesAsInt() <= 0) {
            throw new IllegalArgumentException("chunkSize must be > 0");
        }
        this.chunkSize = chunkSize;
    }

    public TimeValue getRetryDefaultTime() {
        return retryDefaultTime;
    }


    public void setRetryDefaultTime(TimeValue retryDelayEmpty) {
        this.retryDefaultTime = retryDelayEmpty;
    }


}
