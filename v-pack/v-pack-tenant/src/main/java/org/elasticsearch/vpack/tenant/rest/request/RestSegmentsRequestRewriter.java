package org.elasticsearch.vpack.tenant.rest.request;

import org.elasticsearch.common.Strings;
import org.elasticsearch.vpack.tenant.rest.RestRequestRewriter;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

public class RestSegmentsRequestRewriter implements RestRequestRewriter {

    @Override
    public Optional<ParamsRewriter> paramsRewriter() {
        return Optional.of((params, tenant) -> {
            String pms = params.get("h");
            if (Strings.isEmpty(pms)) {
                params.put("h", _default);
            } else {
                String replace = Arrays.asList(pms.split(",")).stream().filter(
                        e -> !(e.equals("ip") || e.equals("id"))
                ).collect(Collectors.joining(","));
                params.put("h", replace);
            }
            if (!params.containsKey("index")) {
                params.put("index", tenant + ".*");
            }
        });
    }

    private static final String _default = "index,shard,prirep,segment,docs,docs.count,consumed,consumed.memory";
}
