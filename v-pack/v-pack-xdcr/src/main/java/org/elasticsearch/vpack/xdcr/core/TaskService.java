package org.elasticsearch.vpack.xdcr.core;


import java.util.*;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.recovery.DelayRecoveryException;
import org.elasticsearch.vpack.xdcr.XDCRSettings;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.vpack.xdcr.action.stats.NodeXRStats;
import org.elasticsearch.vpack.xdcr.util.SchedulerWrapper;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class TaskService extends AbstractComponent {

    private static final Logger LOGGER = Loggers.getLogger(TaskService.class);
    private static final int TRACK_INTERVAL_SECONDS = 5;

    private final SchedulerWrapper schedule;
    private final ThreadPool threadPool;
    private final TransportService transportService;
    private final XDCRSettings xdcrSettings;
    private ConcurrentHashMap<String, TaskEntry> scheduleds = new ConcurrentHashMap<>();

    @Inject
    public TaskService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool,
                       TransportService transportService) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.xdcrSettings = new XDCRSettings(settings, clusterSettings);
        this.schedule = new SchedulerWrapper(threadPool);
    }

    public void schedule(String repository, String index, IndexShard shard, Consumer<Exception> failureConsumer) {
        String key = key(repository, index, shard.shardId().id());
        if (scheduleds.contains(key)) {
            throw new DelayRecoveryException(
                    "cross recovery with same target already registered, waiting for " +
                            "previous recovery attempt to be cancelled or completed");
        }

        RemoteNodeProxy remoteNodeProxy = new RemoteNodeProxy(transportService, threadPool, repository, shard.shardId());
        ChunkSentTask chunkSentTask = new ChunkSentTask(remoteNodeProxy, shard, xdcrSettings);
        ChunkSentTrackerTask chunkSentTrackerTask = new ChunkSentTrackerTask(remoteNodeProxy, shard);

        // 启动同步任务
        Scheduler.Cancellable chunkSentThread = schedule.scheduleWithFixedDelay(chunkSentTask, TimeValue.timeValueMillis(1), ThreadPool.Names.GENERIC,
                (e) -> {
                    cancel(repository, index, shard.shardId().id());
                    failureConsumer.accept(e);
                }
        );
        // 启动同步任务检查器
        Scheduler.Cancellable chunkSentTrackerThread = schedule.scheduleWithFixedDelay(chunkSentTrackerTask, TimeValue.timeValueSeconds(TRACK_INTERVAL_SECONDS), ThreadPool.Names.GENERIC,
                (e) -> {
                    cancel(repository, index, shard.shardId().id());
                    failureConsumer.accept(e);
                }
        );
        scheduleds.put(key, new TaskEntry(chunkSentThread, chunkSentTask, chunkSentTrackerThread, chunkSentTrackerTask));
    }

    public void cancel(String repository, String index, int shard) {
        String key = key(repository, index, shard);
        TaskEntry entry = scheduleds.get(key);
        if (entry != null) {
            entry.chunkSentTask.cancel("cancel");
            entry.chunkSentThread.cancel();
            entry.chunkSentTrackerThread.cancel();
            scheduleds.remove(key);
            LOGGER.info("[xdcr] pushing task cancelled - shard[{}]  ", key);
        }
    }

    public NodeXRStats stats(boolean filterWrong) {
        List<ChunkSentStat> records = listStats(filterWrong);
        return new NodeXRStats(transportService.getLocalNode(), records);
    }

    private List<ChunkSentStat> listStats(boolean filterBlocked) {
        if (scheduleds.isEmpty()) {
            return Collections.EMPTY_LIST;
        }
        List<ChunkSentStat> result = new ArrayList<>();
        scheduleds.values().forEach(e -> {
            ChunkSentStat rec = new ChunkSentStat(
                    e.chunkSentTrackerTask.getRepository(),
                    e.chunkSentTrackerTask.getShard().shardId(),
                    e.chunkSentTrackerTask.getLastRecord().localCheckpoint,
                    e.chunkSentTrackerTask.getLastRecord().remoteCheckpoint,
                    e.chunkSentTrackerTask.getStatus(),
                    e.chunkSentTrackerTask.getErrors());
            if (filterBlocked) {
                if (e.chunkSentTrackerTask.getStatus().equals(ChunkSentTrackerTask.STATUS_BLOCKED)) {
                    result.add(rec);
                }
            } else {
                result.add(rec);
            }
        });
        return result;
    }

    private String key(String repository, String index, int shard) {
        return repository + "-" + index + "-" + shard;
    }

    private static class TaskEntry {

        final Scheduler.Cancellable chunkSentThread;
        final Scheduler.Cancellable chunkSentTrackerThread;
        final ChunkSentTask chunkSentTask;
        final ChunkSentTrackerTask chunkSentTrackerTask;

        public TaskEntry(Scheduler.Cancellable chunkSentThread, ChunkSentTask chunkSentTask,
                         Scheduler.Cancellable chunkSentTrackerThread, ChunkSentTrackerTask chunkSentTrackerTask) {
            this.chunkSentThread = chunkSentThread;
            this.chunkSentTask = chunkSentTask;
            this.chunkSentTrackerThread = chunkSentTrackerThread;
            this.chunkSentTrackerTask = chunkSentTrackerTask;
        }
    }

}
