package org.elasticsearch.vpack.xdcr.task.cluster;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.vpack.XDCRPlugin;

import java.util.Map;

public class ClusterSyncTask extends AllocatedPersistentTask {

    private final Client client;
    private final ClusterService clusterService;
    private final ClusterSyncTaskParams params;
    private final ThreadPool threadPool;
    private Scheduler.Cancellable task;

    public ClusterSyncTask(long id, String type, String action, String description,
                           TaskId parentTask, Map<String, String> headers, ClusterSyncTaskParams params,
                           ClusterService clusterService, ThreadPool threadPool, Client client) {
        super(id, type, action, description, parentTask, headers);
        this.params = params;
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
    }

    public void start() {
        ClusterSyncThread clusterSyncThread = new ClusterSyncThread(clusterService, client, params.repository(), params.excludes());
        task = threadPool.scheduleWithFixedDelay(clusterSyncThread, TimeValue.timeValueSeconds(5), ThreadPool.Names.GENERIC);
    }

    /**
     * 当外部取消任务
     */
    @Override
    protected void onCancelled() {
        task.cancel();
        markAsCompleted();
    }

}
