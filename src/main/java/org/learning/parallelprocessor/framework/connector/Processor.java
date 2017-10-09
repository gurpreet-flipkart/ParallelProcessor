package org.learning.parallelprocessor.framework.connector;


import com.google.common.collect.ImmutableList;
import org.learning.parallelprocessor.framework.ISink;
import org.learning.parallelprocessor.framework.merger.Splitable;
import org.learning.parallelprocessor.framework.step.ListMapping;
import org.learning.parallelprocessor.framework.step.Mapping;
import org.learning.parallelprocessor.framework.step.Reduction;
import org.learning.parallelprocessor.framework.step.StreamStep;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.learning.parallelprocessor.framework.Launcher.THREADPRINT;

public class Processor<Instance, Output> implements Connector<Instance, Output> {
    public static final Object POISON_PILL = new Object();
    private ListMapping<Instance, Output> task;
    private int batchSize;
    private BlockingQueue outputQueue = new LinkedBlockingQueue<>();
    private BlockingQueue inputQueue;
    private AtomicInteger submittedBatched = new AtomicInteger(0);
    private final ExecutorService pool = Executors.newCachedThreadPool();

    /*
    * speedUp 1/(f+(1-f)/p)
    * where p = degree of parallelism.
    * f is the cost of non parallelizable work.
    * */
    public Processor(ListMapping<Instance, Output> computation, int batchSize) {
        this.task = computation;
        this.batchSize = batchSize;
    }

    public Processor(Reduction<Instance, Output> computation, int batchSize) {
        this.task = transform(computation);
        this.batchSize = batchSize;
    }

    private ListMapping<Instance, Output> transform(Reduction<Instance, Output> reduction) {
        return ts -> ImmutableList.of(reduction.process(ts));
    }

    private ListMapping<Instance, Output> transform(Mapping<Instance, Output> mapping) {
        return ts -> ImmutableList.of(mapping.process(ts.get(0)));
    }

    public Processor(Mapping<Instance, Output> task) {
        this.task = transform(task);
        this.batchSize = 1;
    }

    //public Processor(StreamStep<Instance, Output> task) {
    //this.task = task;
    //}


    int submittedInstances = 0;
    private final AtomicBoolean isDone = new AtomicBoolean(false);

    private void lockAndDo() throws Exception {
        synchronized (submittedBatched) {
            submittedBatched.decrementAndGet();
            stop();
        }
    }


    public void run() {
        try {
            if (task instanceof StreamStep) {
                while (!Thread.currentThread().isInterrupted()) {
                    Object instance = inputQueue.take();
                    if (instance == POISON_PILL) {
                        break;
                    }
                    Instance si = (Instance) instance;
                    pool.submit(() -> {
                        try {
                            //to be fixed.
                            ((StreamStep) task).process(si, outputQueue);
                        } catch (Exception e) {
                            e.printStackTrace();
                            //TODO how to handle this scenario gracefully.
                        }
                    });
                    submittedBatched.incrementAndGet();
                    submittedInstances += 1;
                    THREADPRINT.println("Submitted " + submittedBatched.get() + " batches with " + submittedInstances + " instances");
                }
            } else {
                final List<Instance> batch = new LinkedList<>();
                while (!Thread.currentThread().isInterrupted()) {
                    Object instance = inputQueue.take();
                    if (instance == POISON_PILL) {
                        if (!batch.isEmpty()) {
                            work(batch);
                        }
                        break;
                    }
                    Instance si = (Instance) instance;
                    if (si instanceof Splitable) {
                        List<Instance> splits = ((Splitable<Instance>) (si)).split(4000);
                        for (Instance i : splits) {
                            batch.add(i);
                            submit(batch);
                        }
                    } else {
                        batch.add(si);
                        submit(batch);
                    }

                }
            }
            isDone.set(true);
            stop();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    private void stop() throws InterruptedException {
        System.out.println("Remaining Tasks :" + submittedBatched.get());
        if (submittedBatched.get() == 0 && isDone.get()) {
            outputQueue.put(POISON_PILL);
            System.out.println("Processor Added Poison Pill");
        }
    }

    private void submit(List<Instance> batch) {
        if (batch.size() == batchSize) {
            work(batch);
        }
    }


    private void work(List<Instance> batch) {
        pool.submit(getTask(new ArrayList<>(batch)));
        submittedBatched.incrementAndGet();
        submittedInstances += batch.size();
        THREADPRINT.println("Submitted " + submittedBatched.get() + " batches with " + submittedInstances + " instances");
        batch.clear();
    }

    private Runnable getTask(ArrayList<Instance> list) {
        return endAwareStep.apply(task).apply(list);
    }

    private Function<ListMapping<Instance, Output>, Function<List<Instance>, Runnable>> endAwareStep = step -> (List<Instance> instance) -> () -> {
        try {
            try {
                List<Output> output = step.process(instance);
                if (output != null) {
                    for (Output o : output) {
                        outputQueue.put(o);
                    }
                }
            } catch (Exception e) {
                System.err.println("Batch Failed due to :");
                e.printStackTrace();
            }
            lockAndDo();
        } catch (Exception e) {
            e.printStackTrace();
        }
    };

    @Override
    public void pipe(ISink<Output> next) {
        next.setInputQueue(this.getOutputQueue());
    }

    public <Y> Connector<Output, Y> pipe(Connector<Output, Y> next) {
        next.setInputQueue(this.getOutputQueue());
        return next;
    }


    @Override
    public void setInputQueue(BlockingQueue<Instance> inputQueue) {
        this.inputQueue = inputQueue;
    }

    @Override
    public BlockingQueue<Output> getOutputQueue() {
        return outputQueue;
    }


}
