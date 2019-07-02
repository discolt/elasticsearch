package org.elasticsearch.vpack.xdcr.utils;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;

import java.util.concurrent.TimeUnit;

public class Utils {

    public static boolean remoteClusterPrepared(Client client, String repository) {
        try {
            Client remote = client.getRemoteClusterClient(repository);
            remote.admin().cluster().prepareClusterStats().get(TimeValue.timeValueSeconds(1));
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public static void sleep(int second) {
        try {
            TimeUnit.SECONDS.sleep(second);
        } catch (InterruptedException e) {
        }
    }

    public static boolean hasTask(String taskId, ClusterState clusterState) {
        PersistentTasksCustomMetaData.Builder builder = PersistentTasksCustomMetaData.builder(clusterState.getMetaData().custom(PersistentTasksCustomMetaData.TYPE));
        return builder.hasTask(taskId);
    }

}
