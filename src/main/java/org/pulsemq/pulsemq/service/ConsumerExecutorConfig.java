package org.pulsemq.pulsemq.service;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

@Configuration
public class ConsumerExecutorConfig {

    @Bean(destroyMethod = "shutdown")
    public ExecutorService consumerWaitExecutor() {
        AtomicInteger counter = new AtomicInteger();
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName("pulsemq-consumer-wait-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
        return Executors.newCachedThreadPool(threadFactory);
    }
}

