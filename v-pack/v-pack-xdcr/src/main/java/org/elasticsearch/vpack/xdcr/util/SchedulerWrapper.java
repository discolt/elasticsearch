package org.elasticsearch.vpack.xdcr.util;


import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.Scheduler;

import java.util.function.Consumer;


public class SchedulerWrapper {

    private static final Logger LOGGER = Loggers.getLogger(SchedulerWrapper.class);

    private final Scheduler scheduler;

    public SchedulerWrapper(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    public Scheduler.Cancellable scheduleWithFixedDelay(Runnable command, TimeValue interval, String executor, Consumer<Exception> failureConsumer) {
        return new ReschedulingRunnable(command, interval, executor, scheduler,
                (e) -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(() -> new ParameterizedMessage("scheduled task [{}] was rejected on thread pool [{}]",
                                command, executor), e);
                    }
                },
                failureConsumer);
    }

    final class ReschedulingRunnable extends AbstractRunnable implements Scheduler.Cancellable {

        private final Runnable runnable;
        private final TimeValue interval;
        private final String executor;
        private final Scheduler scheduler;
        private final Consumer<Exception> rejectionConsumer;
        private final Consumer<Exception> failureConsumer;

        private volatile boolean run = true;

        /**
         * Creates a new rescheduling runnable and schedules the first execution to occur after the interval specified
         *
         * @param runnable  the {@link Runnable} that should be executed periodically
         * @param interval  the time interval between executions
         * @param executor  the executor where this runnable should be scheduled to run
         * @param scheduler the {@link Scheduler} instance to use for scheduling
         */
        ReschedulingRunnable(Runnable runnable, TimeValue interval, String executor, Scheduler scheduler,
                             Consumer<Exception> rejectionConsumer, Consumer<Exception> failureConsumer) {
            this.runnable = runnable;
            this.interval = interval;
            this.executor = executor;
            this.scheduler = scheduler;
            this.rejectionConsumer = rejectionConsumer;
            this.failureConsumer = failureConsumer;
            scheduler.schedule(interval, executor, this);
        }

        @Override
        public void cancel() {
            run = false;
        }

        @Override
        public boolean isCancelled() {
            return run == false;
        }

        @Override
        public void doRun() {
            // always check run here since this may have been cancelled since the last execution and we do not want to run
            if (run) {
                runnable.run();
            }
        }

        @Override
        public void onFailure(Exception e) {
            failureConsumer.accept(e);
        }

        @Override
        public void onRejection(Exception e) {
            run = false;
            rejectionConsumer.accept(e);
        }

        @Override
        public void onAfter() {
            // if this has not been cancelled reschedule it to run again
            if (run) {
                try {
                    scheduler.schedule(interval, executor, this);
                } catch (final EsRejectedExecutionException e) {
                    onRejection(e);
                }
            }
        }
    }

}
