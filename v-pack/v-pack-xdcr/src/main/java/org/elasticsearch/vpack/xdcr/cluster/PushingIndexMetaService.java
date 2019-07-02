package org.elasticsearch.vpack.xdcr.cluster;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.vpack.xdcr.core.MetaDataUpdator;
import org.elasticsearch.vpack.xdcr.metadata.Feed;
import org.elasticsearch.vpack.xdcr.metadata.FeedInProgress;
import org.elasticsearch.vpack.xdcr.util.SchedulerWrapper;

import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.List;

/**
 * 同步索引元数据
 */
public class PushingIndexMetaService extends AbstractComponent implements ClusterStateListener, IndexEventListener {

    private final TransportService transportService;
    private final ClusterService clusterService;
    private final SchedulerWrapper cancellableScheduler;
    private final ConcurrentHashMap<String, Scheduler.Cancellable> scheduledTasks = new ConcurrentHashMap<>();

    @Inject
    public PushingIndexMetaService(Settings settings, ClusterService clusterService, TransportService transportService, ThreadPool threadPool) {
        super(settings);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.cancellableScheduler = new SchedulerWrapper(threadPool);
        this.clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (!event.localNodeMaster()) {
            cleanAllTasks();
            return;
        }

        try {
            FeedInProgress curr = event.state().custom(FeedInProgress.TYPE);
            if (curr == null) {
                return;
            }
            // 需要新增的
            curr.entries().forEach(e -> {
                if (!scheduledTasks.containsKey(e.feed().key())) {
                    scheduleTask(e.feed());
                }
            });

            // 需要删除的
            List<String> deleted = scheduledTasks.keySet().stream().filter(p -> !curr.hasFeed(p)).collect(Collectors.toList());
            deleted.forEach(e -> {
                cancelTask(e);
            });

        } catch (Exception e) {
            logger.warn("Failed to update index state ", e);
        }
    }

    private void scheduleTask(Feed feed) {
        // 注册metadata更新器
        Scheduler.Cancellable updateMetadataTask = cancellableScheduler.scheduleWithFixedDelay(
                () -> {
                    new MetaDataUpdator(clusterService, transportService, feed.getRepository(), feed.getFeedId().getName()).createOrUpdateOnFollower();
                },
                TimeValue.timeValueSeconds(1),
                ThreadPool.Names.GENERIC,
                (e) -> {
                    logger.error("[xdcr]update index metadata error:", e);
                }
        );
        scheduledTasks.put(feed.key(), updateMetadataTask);
    }

    private void cancelTask(String key) {
        Scheduler.Cancellable task = scheduledTasks.get(key);
        if (task != null) {
            scheduledTasks.get(key).cancel();
            scheduledTasks.remove(key);
        }
    }

    private void cleanAllTasks() {
        if (!scheduledTasks.isEmpty()) {
            scheduledTasks.forEach((k, v) -> {
                cancelTask(k);
            });
        }
    }


}
