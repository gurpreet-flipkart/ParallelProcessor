package org.learning.parallelprocessor.framework;

import java.util.concurrent.BlockingQueue;

public interface ISink<T> extends Component {
    void setInputQueue(BlockingQueue<T> queue);
}
