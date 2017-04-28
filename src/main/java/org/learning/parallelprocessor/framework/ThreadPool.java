package org.learning.parallelprocessor.framework;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;

public interface ThreadPool {
    ExecutorService taskExecutor = Executors.newFixedThreadPool(40);

    static <T> Future<T> async(Supplier<T> supplier) {
        return ThreadPool.taskExecutor.submit(() -> supplier.get());
    }

    static <T> Future<T> submit(Callable<T> callable) {
        return taskExecutor.submit(callable);
    }

    static void async(Runnable callable) {
        taskExecutor.execute(callable);
    }
}
