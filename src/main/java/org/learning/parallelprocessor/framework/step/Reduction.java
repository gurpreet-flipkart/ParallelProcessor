package org.learning.parallelprocessor.framework.step;

import java.util.List;

public interface Reduction<Instance, Output> extends Step<Instance, Output> {
    Output process(final List<Instance> instances) throws Exception;
}
