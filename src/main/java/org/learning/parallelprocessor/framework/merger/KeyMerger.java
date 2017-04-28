package org.learning.parallelprocessor.framework.merger;


import java.util.HashMap;
import java.util.Map;

public class KeyMerger<T extends Mergeable<T>> implements Merger<T> {
    private Map<String, T> map = new HashMap<>();

    @Override
    public Map<String, T> merge(T partialOutput) {
        if (!map.containsKey(partialOutput.getKey())) {
            map.put(partialOutput.getKey(), partialOutput);
        } else {
            map.put(partialOutput.getKey(), map.get(partialOutput.getKey()).merge(partialOutput));
        }
        return map;
    }
}

