package org.learning.parallelprocessor.framework;

import java.util.concurrent.BlockingQueue;

public interface ISink<T> extends Component {
    void setInput(BlockingQueue<T> queue);

    //void pipe(ISink<T> next);

}
