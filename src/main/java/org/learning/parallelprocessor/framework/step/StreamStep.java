package org.learning.parallelprocessor.framework.step;

import java.util.Collection;

public interface StreamStep<Instance, Output> extends Step<Instance, Output> {
    void process(final Instance input, Collection<Output> outputList) throws Exception;
}
