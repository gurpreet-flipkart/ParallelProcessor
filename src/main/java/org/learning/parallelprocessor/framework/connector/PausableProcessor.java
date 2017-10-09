package org.learning.parallelprocessor.framework.connector;

import org.learning.parallelprocessor.framework.Engine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class PausableProcessor<T, V> extends BaseConnector<T, V> implements Engine {
    private static final Logger logger =
            LoggerFactory.getLogger(PausableProcessor.class);
    private final Thread thread;
    private final String name;
    private final AtomicInteger shouldProcess = new AtomicInteger(1);
    private final Function<T, Optional<V>> mapFunction;
    private BlockingQueue<T> upgradableQueue;
    private final Semaphore lock = new Semaphore(1);
    private final Semaphore pauseSemaphore = new Semaphore(1);
    private final BlockingQueue outputQueue = new LinkedBlockingQueue<>();

    private BlockingQueue inputQueue;

    public PausableProcessor(Function<T, Optional<V>> mapFunction, BlockingQueue<T> upgradableQueue, String name) {
        this.mapFunction = mapFunction;
        this.upgradableQueue = upgradableQueue;
        thread = new Thread(() -> run(), name);
        this.name = name;
    }


    public void run() {
        T elem = null;
        Optional<V> result = Optional.empty();
        while (shouldProcess.get() == 1) {
            try {
                this.lock.acquireUninterruptibly();
                while ((elem = this.upgradableQueue.take()) != null) {
                    result = this.mapFunction.apply(elem);
                    if (!result.isPresent()) {
                        break;
                    }
                }
            } catch (InterruptedException e) {
                logger.error("Streaming Consumer run Interrupted " + e.getMessage(), e);
            }
            if (!result.isPresent() && elem != null) {
                if (Thread.currentThread().isInterrupted()) {
                    Thread.interrupted();
                }
                this.mapFunction.apply(elem);
            }
            logger.warn("Drain signal received by distributor. Draining the queue and pausing!");
            drain();
            this.lock.release();
            this.pauseSemaphore.acquireUninterruptibly();
        }
        logger.info("draining before  stopping  distributor!!!");
        drain();
        logger.info("Stopped distributor!!!");
        //resetting state.
        shouldProcess.decrementAndGet();
    }

    private void drain() {
        T elem;
        while ((elem = this.upgradableQueue.poll()) != null) {
            if (!mapFunction.apply(elem).isPresent()) {
                break;
            }
        }
    }

    @Override
    public void pause() {
        this.pauseSemaphore.acquireUninterruptibly();
        this.thread.interrupt();
        this.lock.acquireUninterruptibly();
        this.lock.release();
        logger.warn("Pausing streaming Task:" + this.name);
    }

    @Override
    public void cancel() {
        shouldProcess.incrementAndGet();
        this.thread.interrupt();
    }

    @Override
    public void start() {
        this.thread.start();
    }

    @Override
    public void resume() {
        this.pauseSemaphore.release();
        logger.warn("Resuming Distribution Task");
        System.err.println("resuming  Distribution Task");
    }

}
