package org.learning.parallelprocessor.framework.merger;


import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public class JMerger<T> implements Merger<T> {
    private final Function<T, String> keyFunction;
    private final BiFunction<T, T, T> merger;
    private Map<String, T> map = new HashMap<>();

    public JMerger(Function<T, String> keyFunction, BiFunction<T, T, T> merger) {
        this.keyFunction = keyFunction;
        this.merger = merger;
    }

    @Override
    public Map<String, T> merge(T partialOutput) {
        String key = keyFunction.apply(partialOutput).toString();
        if (!map.containsKey(key)) {
            map.put(key, partialOutput);
        } else {
            map.put(key, merger.apply(map.get(key), (partialOutput)));
        }
        return map;
    }
}

