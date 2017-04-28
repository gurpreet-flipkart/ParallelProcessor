package org.learning.parallelprocessor.framework.merger;

import java.util.ArrayList;
import java.util.List;

public interface Splitable<T> {
    List<T> split(int groups);

    static <T> List<List<T>> split(List<T> scans, int batchSize) {
        int groups = scans.size() / batchSize + 1;
        List<List<T>> splits = new ArrayList<>();
        for (int i = 1; i < groups; i++) {
            List<T> subList = scans.subList((i - 1) * batchSize, i * batchSize);
            splits.add(subList);
        }
        List<T> subList = scans.subList((groups - 1) * batchSize, scans.size());
        splits.add(subList);
        return splits;
    }
}
