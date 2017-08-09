package org.learning.parallelprocessor.framework.connector;


import org.learning.parallelprocessor.framework.ISink;
import org.learning.parallelprocessor.framework.ISource;
import org.learning.parallelprocessor.framework.merger.Key;

public interface Connector<Instance, Output> extends ISource<Output>, ISink<Instance> {
}
