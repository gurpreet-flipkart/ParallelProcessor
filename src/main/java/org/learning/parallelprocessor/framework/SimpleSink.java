package org.learning.parallelprocessor.framework;

import org.learning.parallelprocessor.framework.connector.Processor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class SimpleSink<T> implements ISink<T> {

    private BlockingQueue blockingQueue;
    List<T> accumulator = new ArrayList<>();

    @Override
    public void run() {
        try {
            Object instance;
            while ((instance = blockingQueue.take()) != Processor.POISON_PILL) {
                T si = (T) instance;
                accumulator.add(si);
            }
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void setInputQueue(BlockingQueue<T> queue) {
        this.blockingQueue = queue;
    }

    public List<T> getOutput() {
        return accumulator;
    }
}
