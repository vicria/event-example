package ru.vicria.event.service;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class UncaughtExceptionSchedulers {

    public static Scheduler newSingle(String name, boolean daemon) {
        return Schedulers.newSingle(new SchedulerThreadFactory(name, daemon));
    }

    private static class SchedulerThreadFactory implements ThreadFactory {
        private final AtomicInteger threadCounter = new AtomicInteger(1);
        private final String name;
        private final boolean daemon;


        public SchedulerThreadFactory(String name, boolean daemon) {
            this.name = name;
            this.daemon = daemon;
        }

        @Override
        public Thread newThread(Runnable r) {
            var thread = new Thread(r);
            thread.setDaemon(daemon);
            thread.setName(String.format("%s-%s", name, threadCounter.getAndIncrement()));
            return thread;
        }
    }
}
