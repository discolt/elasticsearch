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

package org.elasticsearch.vpack.xdcr.cluster;

/**
 * Represent Pushing shard index status
 */
public class IndexShardPushingStatus {

    /**
     * Pushing stage
     */
    public enum Stage {
        /**
         * Pushing hasn't started yet
         */
        INIT,
        /**
         * Index files are being copied
         */
        STARTED,
        /**
         * Pushing metadata is being written
         */
        FINALIZE,
        /**
         * Pushing completed successfully
         */
        DONE,
        /**
         * Pushing failed
         */
        FAILURE
    }

    private Stage stage = Stage.INIT;

    private long startTime;

    private long time;

    private int numberOfFiles;

    private volatile int processedFiles;

    private long totalSize;

    private volatile long processedSize;

    private long indexVersion;

    private volatile boolean aborted;

    private String failure;

    /**
     * Returns current index stage
     *
     * @return current index stage
     */
    public Stage stage() {
        return this.stage;
    }

    /**
     * Sets new index stage
     *
     * @param stage new index stage
     */
    public void updateStage(Stage stage) {
        this.stage = stage;
    }

    /**
     * Returns index start time
     *
     * @return index start time
     */
    public long startTime() {
        return this.startTime;
    }

    /**
     * Sets index start time
     *
     * @param startTime index start time
     */
    public void startTime(long startTime) {
        this.startTime = startTime;
    }

    /**
     * Returns index processing time
     *
     * @return processing time
     */
    public long time() {
        return this.time;
    }

    /**
     * Sets index processing time
     *
     * @param time index processing time
     */
    public void time(long time) {
        this.time = time;
    }

    /**
     * Returns true if index process was aborted
     *
     * @return true if index process was aborted
     */
    public boolean aborted() {
        return this.aborted;
    }

    /**
     * Marks index as aborted
     */
    public void abort() {
        this.aborted = true;
    }

    /**
     * Sets files stats
     *
     * @param numberOfFiles number of files in this index
     * @param totalSize     total size of files in this index
     */
    public void files(int numberOfFiles, long totalSize) {
        this.numberOfFiles = numberOfFiles;
        this.totalSize = totalSize;
    }

    /**
     * Sets processed files stats
     *
     * @param numberOfFiles number of files in this index
     * @param totalSize     total size of files in this index
     */
    public synchronized void processedFiles(int numberOfFiles, long totalSize) {
        processedFiles = numberOfFiles;
        processedSize = totalSize;
    }

    /**
     * Increments number of processed files
     */
    public synchronized void addProcessedFile(long size) {
        processedFiles++;
        processedSize += size;
    }

    /**
     * Number of files
     *
     * @return number of files
     */
    public int numberOfFiles() {
        return numberOfFiles;
    }

    /**
     * Total index size
     *
     * @return index size
     */
    public long totalSize() {
        return totalSize;
    }

    /**
     * Number of processed files
     *
     * @return number of processed files
     */
    public int processedFiles() {
        return processedFiles;
    }

    /**
     * Size of processed files
     *
     * @return size of processed files
     */
    public long processedSize() {
        return processedSize;
    }


    /**
     * Sets index version
     *
     * @param indexVersion index version
     */
    public void indexVersion(long indexVersion) {
        this.indexVersion = indexVersion;
    }

    /**
     * Returns index version
     *
     * @return index version
     */
    public long indexVersion() {
        return indexVersion;
    }

    /**
     * Sets the reason for the failure if the index is in the {@link IndexShardPushingStatus.Stage#FAILURE} state
     */
    public void failure(String failure) {
        this.failure = failure;
    }

    /**
     * Returns the reason for the failure if the index is in the {@link IndexShardPushingStatus.Stage#FAILURE} state
     */
    public String failure() {
        return failure;
    }
}
