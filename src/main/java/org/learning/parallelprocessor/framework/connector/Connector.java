package org.learning.parallelprocessor.framework.connector;


import org.learning.parallelprocessor.framework.ISink;
import org.learning.parallelprocessor.framework.ISource;
import org.learning.parallelprocessor.framework.merger.Key;

public interface Connector<Instance, Output> extends ISource<Output>, ISink<Instance> {
    static <X extends Key> void pipe(ISource<X> xProcessor, ISink<X> yProcessor) {
        yProcessor.setInputQueue(xProcessor.getOutputQueue());
    }

    static <X extends Key, Y extends Key> Connector<X, Y> pipe(ISource<X> xProcessor, Connector<X, Y> yProcessor) {
        yProcessor.setInputQueue(xProcessor.getOutputQueue());
        return yProcessor;
    }
}
