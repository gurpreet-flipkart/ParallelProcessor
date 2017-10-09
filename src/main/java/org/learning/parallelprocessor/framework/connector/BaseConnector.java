package org.learning.parallelprocessor.framework.connector;

import org.learning.parallelprocessor.framework.ISink;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class BaseConnector<Instance, Output> implements Connector<Instance, Output> {

    private BlockingQueue outputQueue = new LinkedBlockingQueue<>();
    private BlockingQueue inputQueue;

    @Override
    public void pipe(ISink<Output> next) {
        next.setInputQueue(this.getOutputQueue());
    }

    public <Y> Connector<Output, Y> pipe(Connector<Output, Y> next) {
        next.setInputQueue(this.getOutputQueue());
        return next;
    }


    @Override
    public void setInputQueue(BlockingQueue<Instance> inputQueue) {
        this.inputQueue = inputQueue;
    }

    @Override
    public BlockingQueue<Output> getOutputQueue() {
        return outputQueue;
    }

}
