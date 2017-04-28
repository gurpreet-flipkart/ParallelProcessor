package org.learning.parallelprocessor.framework;


import org.learning.parallelprocessor.framework.connector.Connector;
import org.learning.parallelprocessor.framework.merger.Key;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Source<T> implements ISource<T> {

    private Iterator<T> iterator;
    private Class<T> clazz;

    private BlockingQueue<T> outputQueue = new LinkedBlockingQueue<>();

    public Source(Iterator<T> iterator, Class<T> clazz) {
        this.iterator = iterator;
        this.clazz = clazz;
    }

    @Override
    public void pipe(ISink<T> next) {
        next.setInputQueue(this.getOutputQueue());
    }

    public <Y extends Key> Connector<T, Y> pipe(Connector<T, Y> next) {
        next.setInputQueue(this.getOutputQueue());
        return next;
    }

    public void start() throws Exception {
        while (iterator.hasNext()) {
            T next = iterator.next();
            try {
                outputQueue.put(next);
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
        }
        try {
            System.out.println("Source Added Poison bill");
            outputQueue.put(clazz.newInstance());
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Started ");
    }

    @Override
    public BlockingQueue<T> getOutputQueue() {
        return outputQueue;
    }
}
