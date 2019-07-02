package org.elasticsearch.vpack.adaptz;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.List;
import java.util.Set;

public class Adapts {

    public static String getAppName(RestRequest request) {
        List<String> appNameHeader = request.getAllHeaderValues("AppName");
        if (appNameHeader == null || appNameHeader.size() != 1) {
            throw new ElasticsearchSecurityException("Header AppName invalid!", RestStatus.FORBIDDEN);
        }
        return appNameHeader.get(0);
    }


    /**
     * 用户索引转换到真实索引
     *
     * @param alias
     * @param userIndex
     * @return
     */
    public static String toConcreteIndex(String alias, String userIndex) {
        if (userIndex == null || userIndex.trim().length() == 0) {
            return null;
        }
        return alias + "." + userIndex;
    }

    /**
     * 真实索引转换到用户索引
     *
     * @param alias
     * @param realIndex
     * @return
     */
    public static String toUserIndex(String alias, String realIndex) {
        String regex = ".*(\\.).*";
        if (!realIndex.matches(regex)) {
            throw new IllegalArgumentException(realIndex + " does not match {alias}.{index} pattern");
        }
        return realIndex.substring(alias.length() + 1, realIndex.length());
    }

    /**
     * 用户索引转换到真实索引
     *
     * @param alias
     * @param userIndices
     * @return
     */
    public static String[] concreteIndices(String alias, String... userIndices) {
        String[] result = new String[userIndices.length];
        for (int i = 0; i < userIndices.length; i++) {
            result[i] = toConcreteIndex(alias, userIndices[i]);
        }
        return result;
    }

    /**
     * 校验索引是否在别名下
     *
     * @param client      NodeClient
     * @param appName     别名
     * @param concreteIndices 真实索引
     */
    public static void authAppIndices(final NodeClient client, String appName, String... concreteIndices) {
//        if (appName == null && appName.trim().length() == 0) {
//            return;
//        }
//        ImmutableOpenMap<String, List<AliasMetaData>> aliases = client.admin().indices().getAliases(new GetAliasesRequest(appName)).actionGet().getAliases();
//        if (aliases.isEmpty()) {
//            return;
//        }
//        for (String index : concreteIndices) {
//            if (!aliases.keys().contains(index)) {
//                throw new ElasticsearchSecurityException("index [{}] is unauthorized for current user!", RestStatus.FORBIDDEN, toUserIndex(appName, index));
//            }
//        }
    }

    /**
     * 校验索引是否在别名下
     *
     * @param client   NodeClient
     * @param appName  别名
     * @param indexSet 用户索引
     */
    public static void authAppIndices(final NodeClient client, String appName, Set<String> indexSet) {
        String[] indices = indexSet.toArray(new String[indexSet.size()]);
        authAppIndices(client, appName, indices);
    }

}
