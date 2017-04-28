package org.learning.parallelprocessor.framework.task;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class ListMapTask<Output> implements Task {
    protected Callable<List<Output>> processor;
    private Consumer<List<Output>> onComplete;

    public ListMapTask(Callable<List<Output>> processor, Consumer<List<Output>> onComplete) {
        this.processor = processor;
        this.onComplete = onComplete;
    }

    @Override
    public void run() {
        try {
            List<Output> output = this.processor.call();
            onComplete.accept(output);
        } catch (Exception e) {
            System.err.println("Batch Failed due to :");
            e.printStackTrace();
            onComplete.accept(null);
        }
    }
}
