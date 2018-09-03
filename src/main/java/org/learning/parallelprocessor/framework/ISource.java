package org.learning.parallelprocessor.framework;


import org.learning.parallelprocessor.framework.connector.Connector;
import org.learning.parallelprocessor.framework.merger.Key;

import java.util.concurrent.BlockingQueue;

public interface ISource<T> extends Component {

    void pipe(ISink<T> next);

    <Y> Connector<T, Y> pipe(Connector<T, Y> next);

}
