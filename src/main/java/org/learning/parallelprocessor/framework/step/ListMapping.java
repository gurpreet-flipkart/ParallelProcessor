package org.learning.parallelprocessor.framework.step;

import java.util.List;

public interface ListMapping<Instance, Output> extends Step<Instance, Output> {
    List<Output> process(final List<Instance> instances) throws Exception;
}
