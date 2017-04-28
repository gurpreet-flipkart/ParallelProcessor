package org.learning.parallelprocessor.framework;


import org.learning.parallelprocessor.framework.connector.Connector;
import org.learning.parallelprocessor.framework.merger.Key;

import java.util.concurrent.BlockingQueue;

public interface ISource<T> extends Component {

    BlockingQueue<T> getOutputQueue();

    void pipe(ISink<T> next);

    <Y extends Key> Connector<T, Y> pipe(Connector<T, Y> next);

}
