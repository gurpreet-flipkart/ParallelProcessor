package org.learning.parallelprocessor.framework.merger;

public interface Mergeable<T> extends Key {
    T merge(T partialOutput);
}
