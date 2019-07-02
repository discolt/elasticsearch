package org.elasticsearch.vpack.xdcr;


import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.vpack.xdcr.cluster.PushingIndexMetaService;
import org.elasticsearch.vpack.xdcr.cluster.PushingService;
import org.elasticsearch.vpack.xdcr.cluster.PushingShardsService;
import org.elasticsearch.vpack.xdcr.core.TaskService;
import org.elasticsearch.vpack.xdcr.core.follower.FollowerHandlerRegister;

/**
 * Created by discolt on 2018/1/23.
 */
public class XDCRModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(PushingService.class).asEagerSingleton();
        bind(PushingShardsService.class).asEagerSingleton();
        bind(PushingIndexMetaService.class).asEagerSingleton();
        bind(FollowerHandlerRegister.class).asEagerSingleton();
        bind(TaskService.class).asEagerSingleton();

    }
}