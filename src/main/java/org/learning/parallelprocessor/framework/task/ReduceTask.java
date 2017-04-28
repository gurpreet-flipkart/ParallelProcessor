package org.learning.parallelprocessor.framework.task;

import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class ReduceTask<Output> implements Task {
    protected Callable<Output> processor;
    private Consumer<Output> onTaskComplete;

    public ReduceTask(Callable<Output> processor, Consumer<Output> onTaskComplete) {
        this.processor = processor;
        this.onTaskComplete = onTaskComplete;
    }

    @Override
    public void run() {
        try {
            Output output = this.processor.call();
            onTaskComplete.accept(output);
        } catch (Exception e) {
            System.err.println("Batch Failed due to :");
            e.printStackTrace();
            onTaskComplete.accept(null);
        }
    }
}
