package org.learning.parallelprocessor.framework.connector;

import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import org.junit.Assert;
import org.junit.Test;
import org.learning.parallelprocessor.framework.Launcher;
import org.learning.parallelprocessor.framework.SimpleSink;
import org.learning.parallelprocessor.framework.Sink;
import org.learning.parallelprocessor.framework.Source;
import org.learning.parallelprocessor.framework.merger.JMerger;
import org.learning.parallelprocessor.framework.step.ListMapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProcessorTest {

    @Test
    public void test() throws InterruptedException {
        Source<String> source = new Source<>(ImmutableList.of("A", "B", "C", "D", "A", "D").iterator());
        Connector<String, String> connector = new Processor<>(i -> i + "-" + i);
        SimpleSink<String> sink = new SimpleSink<>();
        source.pipe(connector).pipe(sink);
        Launcher.launch(source, connector, sink);
        List<String> output = sink.getOutput();
        Assert.assertEquals(6, output.size());

    }

    @AllArgsConstructor
    static class Shipment {
        int id, bagId;
    }

    @AllArgsConstructor
    static class Bag {
        int bagId;
        List<Integer> shipments;

        @Override
        public String toString() {
            return bagId + "->" + shipments;
        }
    }

    @Test
    public void test2() throws InterruptedException {
        Source<Shipment> source = new Source<>(ImmutableList.of(new Shipment(1, 1), new Shipment(2, 1), new Shipment(3, 1), new Shipment(4, 2)).iterator());
        Connector<Shipment, Bag> connector = new Processor<>((ListMapping<Shipment, Bag>) shipments ->
                shipments.stream().collect(Collectors.groupingBy(s -> s.bagId, Collectors.mapping(s -> s.id, Collectors.toList()))).values().stream().map(list -> new Bag(list.get(0), list)).collect(Collectors.toList()), 2);
        Sink<Bag> iSink = new Sink<>(new JMerger<>(s -> String.valueOf(s.bagId), (x, y) -> {
            List<Integer> merged = new ArrayList(x.shipments);
            merged.addAll(y.shipments);
            return new Bag(x.bagId, merged);
        }));
        source.pipe(connector).pipe(iSink);
        Launcher.launch(source, connector, iSink);
        Map<String, Bag> output = iSink.getOutput();
        Assert.assertEquals(3, output.size());

    }
}