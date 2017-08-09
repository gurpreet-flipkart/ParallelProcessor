package org.learning.parallelprocessor.framework.connector;


import org.learning.parallelprocessor.framework.ISink;
import org.learning.parallelprocessor.framework.merger.Splitable;
import org.learning.parallelprocessor.framework.step.*;
import org.learning.parallelprocessor.framework.task.ListMapTask;
import org.learning.parallelprocessor.framework.task.ReduceTask;
import org.learning.parallelprocessor.framework.task.Task;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.learning.parallelprocessor.framework.Launcher.THREADPRINT;
import static org.learning.parallelprocessor.framework.ThreadPool.async;

public class Processor<Instance, Output> implements Connector<Instance, Output> {
    public static final Object POISON_PILL = new Object();
    private Step<Instance, Output> task;
    private int batchSize;
    private BlockingQueue outputQueue = new LinkedBlockingQueue<>();
    private BlockingQueue inputQueue;
    private AtomicInteger submittedBatched = new AtomicInteger(0);

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
        this.task = computation;
        this.batchSize = batchSize;
    }

    public Processor(Mapping<Instance, Output> task) {
        this.task = task;
    }

    public Processor(StreamStep<Instance, Output> task) {
        this.task = task;
    }


    int submittedInstances = 0;
    private final AtomicBoolean isDone = new AtomicBoolean(false);

    private Consumer<Output> iPublisher = (Output o) -> {
        try {
            if (o != null) {
                outputQueue.put(o);
            }
            lockAndDo();
        } catch (Exception e) {
            e.printStackTrace();
        }
    };


    private Consumer<List<Output>> listPublisher = (List<Output> output) -> {
        try {
            if (output != null) {
                for (Output o : output) {
                    outputQueue.put(o);
                }
            }
            lockAndDo();
        } catch (Exception e) {
            e.printStackTrace();
        }
    };

    private void lockAndDo() throws Exception {
        synchronized (submittedBatched) {
            int count = submittedBatched.decrementAndGet();
            System.out.println("Remaining Tasks :" + count);
            if (count == 0 && isDone.get()) {
                outputQueue.put(POISON_PILL);
                System.out.println("Processor Added Poison Pill");
            }
        }
    }


    public void run() {
        try {
            if (task instanceof Mapping || task instanceof StreamStep) {
                while (!Thread.currentThread().isInterrupted()) {
                    Object instance = inputQueue.take();
                    if (instance == POISON_PILL) {
                        break;
                    }
                    Instance si = (Instance) instance;
                    if (task instanceof Mapping) {
                        if (false) {
                            async(new ReduceTask<>(() -> ((Mapping<Instance, Output>) task).process(si), x -> {
                            }));
                        } else {
                            async(new ReduceTask<>(() -> ((Mapping<Instance, Output>) task).process(si), iPublisher));
                        }
                    } else {
                        async(() -> {
                            try {
                                //to be fixed.
                                ((StreamStep) task).process(si, outputQueue);
                            } catch (Exception e) {
                                e.printStackTrace();
                                //TODO how to handle this scenario gracefully.
                            }
                        });
                    }
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
            System.out.println("Am Done");
            isDone.set(true);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    private void submit(List<Instance> batch) {
        if (batch.size() == batchSize) {
            work(batch);
        }
    }


    private void work(List<Instance> batch) {
        async(getTask(new ArrayList<>(batch)));
        submittedBatched.incrementAndGet();
        submittedInstances += batch.size();
        THREADPRINT.println("Submitted " + submittedBatched.get() + " batches with " + submittedInstances + " instances");
        batch.clear();
    }

    private Task getTask(ArrayList<Instance> copy) {
        if (task instanceof Reduction) {
            /*if (this.clazz == Void.class) {
                return new ReduceTask<>(() -> ((Reduction<Instance, Output>) task).process(copy), x -> {
                });
            }*/
            return new ReduceTask<>(() -> ((Reduction<Instance, Output>) task).process(copy), iPublisher);
        } else if (task instanceof ListMapping) {
            /*if (this.clazz == Void.class) {
                return new ListMapTask<>(() -> ((ListMapping) task).process(copy), x -> {
                });
            }*/
            return new ListMapTask<>(() -> ((ListMapping) task).process(copy), listPublisher);
        }
        return null;
    }

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
