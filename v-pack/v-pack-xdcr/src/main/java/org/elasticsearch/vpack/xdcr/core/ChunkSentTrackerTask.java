package org.elasticsearch.vpack.xdcr.core;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.shard.IndexShard;

import java.util.Date;

public class ChunkSentTrackerTask implements Runnable {

    private static final Logger LOGGER = Loggers.getLogger(ChunkSentTrackerTask.class);

    public static final String STATUS_WORKING = "working";
    public static final String STATUS_BLOCKED = "blocked";

    private RemoteNodeProxy remoteNodeProxy;
    private IndexShard localShard;
    private Record lastRecord;
    private String status = STATUS_WORKING;
    private String errors = "";

    public ChunkSentTrackerTask(RemoteNodeProxy remoteNodeProxy, IndexShard localShard) {
        this.localShard = localShard;
        this.remoteNodeProxy = remoteNodeProxy;
    }

    /**
     * 如果Record RemoteCheckpoint发生变化则放置到队列第一位，并将前面的Record去掉。
     * 未发生变化则放置到第二位。
     */
    @Override
    public void run() {
        try {
            long localCheckpoint = localShard.seqNoStats().getMaxSeqNo();
            long remoteCheckpoint = remoteNodeProxy.fetchGlobalCheckpoint();
            Record record = new Record(new Date(), localCheckpoint, remoteCheckpoint);
            if (lastRecord == null) {
                lastRecord = record;
                status = "INIT";
                return;
            }
            // 如果remoteCheckpoint未发生变化，并且最后一次的记录里remoteCheckpoint与localCheckpoint不一致则算出问题了。
            if (record.remoteCheckpoint == lastRecord.remoteCheckpoint &&
                    lastRecord.remoteCheckpoint != lastRecord.localCheckpoint) {
                status = STATUS_BLOCKED;
            } else {
                status = STATUS_WORKING;
                errors = "";
            }
            lastRecord = record;
        } catch (Exception cause) {
            status = STATUS_BLOCKED;
            errors = cause.getMessage();
        }
    }

    public String getRepository() {
        return remoteNodeProxy.getRepository();
    }

    public Record getLastRecord() {
        return lastRecord;
    }

    public IndexShard getShard() {
        return localShard;
    }

    public String getStatus() {
        return status;
    }

    public String getErrors() {
        return errors;
    }

    public static class Record {
        final Date time;
        final long localCheckpoint;
        final long remoteCheckpoint;

        Record(Date time, long localCheckpoint, long remoteCheckpoint) {
            this.time = time;
            this.localCheckpoint = localCheckpoint;
            this.remoteCheckpoint = remoteCheckpoint;
        }
    }

}
