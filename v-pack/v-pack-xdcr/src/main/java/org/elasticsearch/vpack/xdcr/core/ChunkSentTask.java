package org.elasticsearch.vpack.xdcr.core;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.ElasticsearchWrapperException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.seqno.LocalCheckpointTracker;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.*;
import org.elasticsearch.index.snapshots.IndexShardRestoreException;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.vpack.xdcr.XDCRSettings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ChunkSentTask implements Runnable {

    private static final Logger LOGGER = Loggers.getLogger(ChunkSentTask.class);

    private final IndexShard shard;
    private final RemoteNodeProxy remoteNodeProxy;
    private final XDCRSettings xdcrSettings;

    public ChunkSentTask(RemoteNodeProxy remoteNodeProxy,
                         IndexShard indexShard, XDCRSettings xdcrSettings) {
        this.shard = indexShard;
        this.remoteNodeProxy = remoteNodeProxy;
        this.xdcrSettings = xdcrSettings;
    }

    public IndexShard getShard() {
        return shard;
    }

    public void cancel(String reson) {
        cancellableThreads.cancel(reson);
    }

    @Override
    public void run() {
        try (Closeable ignored = shard.acquireTranslogRetentionLock()) {
            // 获取远程集群Checkpoint
            long remoteCheckpoint = remoteNodeProxy.fetchGlobalCheckpoint();
            if (remoteCheckpoint == SequenceNumbers.UNASSIGNED_SEQ_NO
                    || remoteCheckpoint >= shard.getLocalCheckpoint()) {
                sleep(3000);
                return;
            }

            // 发送Translog
            final long startingSeqNo = remoteCheckpoint < 0 ? 0 : remoteCheckpoint + 1;
            final long requiredSeqNoRangeStart = startingSeqNo;
            String allocationId = shard.routingEntry().allocationId().getId();
            runUnderPrimaryPermit(() -> shard.initiateTracking(allocationId));
            final long endingSeqNo = shard.seqNoStats().getMaxSeqNo();
            cancellableThreads.execute(() -> shard.waitForOpsToComplete(endingSeqNo));
            Translog.Snapshot snapshot = shard.newTranslogSnapshotFromMinSeqNo(startingSeqNo);
            if (shard.state() == IndexShardState.CLOSED) {
                throw new IndexShardClosedException(shard.shardId());
            }
            cancellableThreads.checkForCancel();
            sendBatchChunk(startingSeqNo, requiredSeqNoRangeStart, endingSeqNo, snapshot);

        } catch (Exception e) {

            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(() -> new ParameterizedMessage("RecoveryException, [{}][{}]", shard.shardId(), e));
            }

            Throwable cause = ExceptionsHelper.unwrapCause(e);

            // 需要更新表结构
            if (cause instanceof IllegalIndexShardStateException
                    || cause instanceof IndexNotFoundException
                    || cause instanceof ShardNotFoundException
                    || cause instanceof MapperException) {
                LOGGER.warn("[xdcr] retry recovery task. cause update index metadata required[{}]", shard.shardId().getIndex());
                sleep(1000);
                return;
            }

            // 远端checkpoint不匹配
            if (cause instanceof IndexShardRestoreException) {
                LOGGER.warn("[xdcr] retry recovery task. cause {}", cause);
                return;
            }

            // 失去网络连接
            if (cause instanceof ConnectTransportException) {
                LOGGER.warn("[xdcr] retry recovery task. cause connect lose {} ", cause);
                sleep(3000);
                return;
            }

            if (cause instanceof ClusterBlockException) {
                LOGGER.warn("[xdcr] retry recovery task. cause {}", cause);
                sleep(3000);
                return;
            }

            if (cause instanceof AlreadyClosedException) {
                LOGGER.warn("[xdcr] shard closed.  {}", cause);
                throw ExceptionsHelper.convertToElastic(e);
            }

            LOGGER.warn("[xdcr] retry recovery task after 5sec. cause unknow error: {}", e);
            sleep(5000);
            e.printStackTrace();
        }
    }

    /**
     * 发送Translog
     *
     * @param startingSeqNo
     * @param requiredSeqNoRangeStart
     * @param endingSeqNo
     * @param snapshot
     * @throws IOException
     */
    private void sendBatchChunk(final long startingSeqNo, long requiredSeqNoRangeStart, long endingSeqNo,
                                final Translog.Snapshot snapshot) throws IOException {

        LOGGER.trace("chunk sent task started from [{}] to [{}], total[{}], translog.snapshot[{}]", startingSeqNo, endingSeqNo, snapshot.totalOperations(), snapshot);
        long size = 0;
        final List<Translog.Operation> operations = new ArrayList<>();
        final LocalCheckpointTracker requiredOpsTracker = new LocalCheckpointTracker(endingSeqNo, requiredSeqNoRangeStart - 1);

        final CancellableThreads.IOInterruptable sendBatch =
                () -> {
                    /* 这里获取的MultiSnapshot内部Translog不是保序的，如[1,3,4,6,7,8,5,9,10 ...]
                     * 当发生异常时，如[1,3,4,6] 已发往目标集群，下次任务将从7开始获取,seqNo=5的translog则会发生丢失。
                     * 因此每次发往目标集群的Chunk需要排序，目标集群会检查translog是否连续，若发现则抛出异常重新发起恢复任务。
                     */
                    LOGGER.trace("send translog to proxy, min[{}] max[{}]", operations.get(0).seqNo(), operations.get(operations.size() - 1).seqNo());
                    operations.sort((v1, v2) -> v1.seqNo() < v2.seqNo() ? -1 : v1.seqNo() > v2.seqNo() ? 1 : 0);
                    remoteNodeProxy.indexTranslogOperations(operations);
                };

        // sent translog operations
        long chunkSizeInBytes = xdcrSettings.getChunkSize().getBytes();
        Translog.Operation operation;
        while ((operation = snapshot.next()) != null) {
            if (shard.state() == IndexShardState.CLOSED) {
                throw new IndexShardClosedException(shard.shardId());
            }
            cancellableThreads.checkForCancel();

            final long seqNo = operation.seqNo();
            if (seqNo < startingSeqNo || seqNo > endingSeqNo) {
                continue;
            }
            operations.add(operation);
            size += operation.estimateSize();
            requiredOpsTracker.markSeqNoAsCompleted(seqNo);

            // check if this request is past bytes threshold, and if so, send it off
            if (size >= chunkSizeInBytes) {
                cancellableThreads.executeIO(sendBatch);
                operations.clear();
                size = 0;
            }
        }

        if (!operations.isEmpty()) {
            cancellableThreads.executeIO(sendBatch);
        }

        if (requiredOpsTracker.getCheckpoint() < endingSeqNo) {
            String errorMsg = "translog index failed to send,  required sequence numbers" +
                    " (required range [" + requiredSeqNoRangeStart + "->" + endingSeqNo + "). first missing op is ["
                    + (requiredOpsTracker.getCheckpoint() + 1) + "]";
            LOGGER.error(errorMsg);
            throw new IllegalStateException(errorMsg);
        }
    }

    private final CancellableThreads cancellableThreads = new CancellableThreads() {
        @Override
        protected void onCancel(String reason, @Nullable Exception suppressedException) {
            RuntimeException e;
            if (shard.state() == IndexShardState.CLOSED) { // check if the shard got closed on us
                e = new IndexShardClosedException(shard.shardId(), "shard is closed and recovery was canceled reason [" + reason + "]");
            } else {
                e = new ExecutionCancelledException("recovery was canceled reason [" + reason + "]");
            }
            if (suppressedException != null) {
                e.addSuppressed(suppressedException);
            }
            throw e;
        }
    };

    private void runUnderPrimaryPermit(CancellableThreads.Interruptable runnable) {
        cancellableThreads.execute(() -> {
            final PlainActionFuture<Releasable> onAcquired = new PlainActionFuture<>();
            shard.acquirePrimaryOperationPermit(onAcquired, ThreadPool.Names.SAME, "cross cluster startRecovery");
            try (Releasable ignored = onAcquired.actionGet()) {
                runnable.run();
            }
        });
    }

    private static void sleep(long ms) {
        try {
            TimeUnit.MILLISECONDS.sleep(ms);
        } catch (InterruptedException e) {
        }
    }

}
