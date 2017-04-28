package org.learning.parallelprocessor.framework.merger;

import java.util.Map;

public interface Merger<T> {
    Map<String, T> merge(T partialOutput);
}
