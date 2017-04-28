package org.learning.parallelprocessor.framework.step;


public interface Mapping<Instance, Output> extends Step<Instance, Output> {
    Output process(final Instance instances) throws Exception;
}
