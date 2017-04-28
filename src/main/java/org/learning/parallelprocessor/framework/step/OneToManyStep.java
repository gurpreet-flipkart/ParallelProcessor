package org.learning.parallelprocessor.framework.step;

import java.util.List;

public interface OneToManyStep<Instance, Output> extends Step<Instance, Output> {
    List<Output> process(final Instance input) throws Exception;
}
