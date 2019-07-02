package org.elasticsearch.vpack.xdcr.task.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

public class IndexSyncExecutor extends PersistentTasksExecutor<IndexSyncTaskParams> {

    private static final Logger logger = LogManager.getLogger(IndexSyncExecutor.class);

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final Client client;

    /**
     * 执行 SyncTaskParams
     *
     * @param clusterService
     * @param client
     */
    public IndexSyncExecutor(ClusterService clusterService, ThreadPool threadPool, Client client) {
        super(IndexSyncTaskParams.NAME, XDCR_THREAD_POOL_NAME);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.client = client;
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, IndexSyncTaskParams params, PersistentTaskState state) {
        IndexSyncTask indexSyncTask = (IndexSyncTask) task;
        indexSyncTask.start();
    }

    @Override
    protected AllocatedPersistentTask createTask(long id, String type, String action, TaskId parentTaskId,
                                                 PersistentTasksCustomMetaData.PersistentTask<IndexSyncTaskParams> taskInProgress,
                                                 Map<String, String> headers) {
        IndexSyncTaskParams params = taskInProgress.getParams();
        return new IndexSyncTask(id, type, action, "xdcr-sync-index", parentTaskId, headers, params, clusterService, threadPool, client);
    }

}
