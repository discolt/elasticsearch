package org.elasticsearch.vpack.xdcr.task.cluster;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

import static org.elasticsearch.vpack.XDCRPlugin.XDCR_THREAD_POOL_NAME;

public class ClusterSyncExecutor extends PersistentTasksExecutor<ClusterSyncTaskParams> {

    private final Client client;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;

    public ClusterSyncExecutor(ClusterService clusterService, ThreadPool threadPool, Client client) {
        super(ClusterSyncTaskParams.NAME, XDCR_THREAD_POOL_NAME);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.client = client;
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, ClusterSyncTaskParams params, PersistentTaskState state) {
        ClusterSyncTask clusterSyncTask = (ClusterSyncTask) task;
        clusterSyncTask.start();
    }

    @Override
    protected AllocatedPersistentTask createTask(long id, String type, String action, TaskId parentTaskId,
                                                 PersistentTasksCustomMetaData.PersistentTask<ClusterSyncTaskParams> taskInProgress,
                                                 Map<String, String> headers) {
        ClusterSyncTaskParams params = taskInProgress.getParams();
        return new ClusterSyncTask(id, type, action, "xdcr-sync-cluster", parentTaskId, headers, params, clusterService, threadPool, client);
    }

}
