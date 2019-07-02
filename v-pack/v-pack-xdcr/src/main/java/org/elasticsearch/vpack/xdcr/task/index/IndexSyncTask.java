package org.elasticsearch.vpack.xdcr.task.index;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.vpack.XDCRPlugin;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IndexSyncTask extends AllocatedPersistentTask {

    private final Client client;
    private final ClusterService clusterService;
    private final IndexSyncTaskParams params;
    private final ThreadPool threadPool;
    private final Set<Scheduler.Cancellable> runningTasks = new HashSet<>();

    public IndexSyncTask(long id, String type, String action, String description,
                         TaskId parentTask, Map<String, String> headers, IndexSyncTaskParams params,
                         ClusterService clusterService, ThreadPool threadPool, Client client) {
        super(id, type, action, description, parentTask, headers);
        this.params = params;
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    public void start() {
        IndexMetaDataSyncThread metaTask = new IndexMetaDataSyncThread(clusterService, client, params);
        Scheduler.Cancellable task = threadPool.scheduleWithFixedDelay(metaTask, TimeValue.timeValueSeconds(3), ThreadPool.Names.GENERIC);
        runningTasks.add(task);

        IndexMetaData metaData = clusterService.state().metaData().index(params.index());
        if (metaData != null) {
            int shardNums = metaData.getNumberOfShards();
            for (int i = 0; i < shardNums; i++) {
                IndexShardDeliveryThread shardDeliveryThread = new IndexShardDeliveryThread(clusterService, client, params, i);
                Scheduler.Cancellable shardTask = threadPool.scheduleWithFixedDelay(shardDeliveryThread, TimeValue.timeValueSeconds(2), XDCRPlugin.XDCR_THREAD_POOL_NAME);
                runningTasks.add(shardTask);
            }
        }
    }

    /**
     * 当外部取消任务
     */
    @Override
    protected void onCancelled() {
        runningTasks.forEach(e -> {
            e.cancel();
        });
        markAsCompleted();
    }


}
