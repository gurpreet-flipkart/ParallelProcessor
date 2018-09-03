package org.learning.parallelprocessor.framework;


import org.learning.parallelprocessor.framework.connector.Processor;
import org.learning.parallelprocessor.framework.merger.Key;
import org.learning.parallelprocessor.framework.merger.Merger;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;

public class Sink<T> implements ISink<T> {
    private BlockingQueue<T> inputQueue = null;
    private Merger<T> merger;

    public Sink(Merger<T> merger) {
        this.merger = merger;
    }

    private Future<Map<String, T>> submitted = null;

    public void run(){
        submitted = ThreadPool.submit(() -> {
            Map<String, T> output = null;
            while (true) {
                T partialOutput = inputQueue.take();
                if (partialOutput == Processor.POISON_PILL) {
                    break;
                }
                output = merger.merge(partialOutput);
            }
            return output;
        });
    }

    public Map<String, T> getOutput() {
        try {
            return submitted.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void setInput(BlockingQueue<T> queue) {
        this.inputQueue = queue;
    }
}
