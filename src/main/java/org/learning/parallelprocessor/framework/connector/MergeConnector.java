package org.learning.parallelprocessor.framework.connector;


import org.learning.parallelprocessor.framework.ISink;
import org.learning.parallelprocessor.framework.Sink;
import org.learning.parallelprocessor.framework.merger.Merger;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class MergeConnector<T> extends Sink<T> implements Connector<T, T> {

    public MergeConnector(Merger<T> merger) {
        super(merger);
    }


    BlockingQueue outputQueue = new LinkedBlockingQueue<>();



    public BlockingQueue<T> getOutputQueue() {
        return outputQueue;
    }


    @Override
    public void pipe(ISink<T> next) {
        next.setInputQueue(this.getOutputQueue());
    }

    public <Y> Connector<T, Y> pipe(Connector<T, Y> next) {
        next.setInputQueue(this.getOutputQueue());
        return next;
    }

    @Override
    public void run(){
        try {
            MergeConnector.super.run();
            Collection<T> values = getOutput().values();
            for (T v : values) {
                outputQueue.put(v);
            }
            System.out.println("\n Merger added poison pill of Type");
            outputQueue.put(Processor.POISON_PILL);
        } catch (Exception e) {
            e.printStackTrace();
            try {
                outputQueue.put(Processor.POISON_PILL);
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
    }
}
