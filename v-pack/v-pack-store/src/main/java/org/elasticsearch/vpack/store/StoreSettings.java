package org.elasticsearch.vpack.store;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.Arrays;
import java.util.List;

public class StoreSettings {

    private volatile ByteSizeValue queueLimitSize;
    private volatile ByteSizeValue bulkBufferSize;

    public StoreSettings(Settings settings, ClusterService clusterService) {
        this.queueLimitSize = QUEUE_LIMIT_SIZE_SETTINGS.get(settings);
        this.bulkBufferSize = BULK_BUFFER_SIZE_SETTINGS.get(settings);
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        clusterSettings.addSettingsUpdateConsumer(QUEUE_LIMIT_SIZE_SETTINGS, this::setQueueLimitSize);
        clusterSettings.addSettingsUpdateConsumer(BULK_BUFFER_SIZE_SETTINGS, this::setBulkBufferSize);
    }

    public ByteSizeValue getQueueLimitSize() {
        return queueLimitSize;
    }

    public void setQueueLimitSize(ByteSizeValue queueLimitSize) {
        this.queueLimitSize = queueLimitSize;
    }

    public ByteSizeValue getBulkBufferSize() {
        return bulkBufferSize;
    }

    public void setBulkBufferSize(ByteSizeValue bulkBufferSize) {
        this.bulkBufferSize = bulkBufferSize;
    }

    /**
     * 索引远程独立存储配置
     */
    public static final Setting<String> INDEX_STORE_REMOTE_SETTINGS =
            Setting.simpleString("index.store.remote", Setting.Property.IndexScope, Setting.Property.InternalIndex);

    /**
     * 队列大小，超出范围则不存储远程。
     */
    public static final Setting<ByteSizeValue> QUEUE_LIMIT_SIZE_SETTINGS =
            Setting.byteSizeSetting("index.store.queue_limit_size", new ByteSizeValue(32, ByteSizeUnit.MB), Setting.Property.Dynamic, Setting.Property.NodeScope);

    /**
     * 发往远程集群的Bulk-Buffer大小
     */
    public static final Setting<ByteSizeValue> BULK_BUFFER_SIZE_SETTINGS =
            Setting.byteSizeSetting("index.store.bulk_size", new ByteSizeValue(8, ByteSizeUnit.MB), Setting.Property.Dynamic, Setting.Property.NodeScope);


    public static List<Setting<?>> getSettings() {
        return Arrays.asList(QUEUE_LIMIT_SIZE_SETTINGS, BULK_BUFFER_SIZE_SETTINGS, INDEX_STORE_REMOTE_SETTINGS);
    }


}
