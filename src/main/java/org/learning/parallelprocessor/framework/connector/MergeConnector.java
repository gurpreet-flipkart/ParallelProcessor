package org.learning.parallelprocessor.framework.connector;


import org.learning.parallelprocessor.framework.ISink;
import org.learning.parallelprocessor.framework.Sink;
import org.learning.parallelprocessor.framework.merger.Key;
import org.learning.parallelprocessor.framework.merger.Merger;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MergeConnector<T extends Key> extends Sink<T> implements Connector<T, T> {
    private final Class<T> clazz;

    public MergeConnector(Merger<T> merger, Class<T> clazz) {
        super(merger);
        this.clazz = clazz;
    }


    BlockingQueue<T> outputQueue = new LinkedBlockingQueue<>();


    @Override
    public BlockingQueue<T> getOutputQueue() {
        return outputQueue;
    }


    @Override
    public void pipe(ISink<T> next) {
        next.setInputQueue(this.getOutputQueue());
    }

    public <Y extends Key> Connector<T, Y> pipe(Connector<T, Y> next) {
        next.setInputQueue(this.getOutputQueue());
        return next;
    }

    @Override
    public void start() throws Exception {
        try {
            MergeConnector.super.start();
            Collection<T> values = getOutput().values();
            for (T v : values) {
                outputQueue.put(v);
            }
            System.out.println("\n Merger added poison pill of Type:" + this.clazz.getSimpleName());
            outputQueue.put(this.clazz.newInstance());
        } catch (Exception e) {
            e.printStackTrace();
            try {
                outputQueue.put(this.clazz.newInstance());
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
    }
}
