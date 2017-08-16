package org.learning.parallelprocessor.framework;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Launcher {
    public static final ThreadPrintStream THREADPRINT = new ThreadPrintStream(System.out);

    public static void launch(Component... components) throws InterruptedException {
        ExecutorService executorService = Executors.newCachedThreadPool();
        for (Component c : components) {
            if (c instanceof ISink) {
                c.run();
            } else {
                executorService.submit(c);
            }
        }
        //TODO implement stop() method.
    }
}
