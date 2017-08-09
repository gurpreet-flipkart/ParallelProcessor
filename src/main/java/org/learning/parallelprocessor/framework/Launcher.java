package org.learning.parallelprocessor.framework;


import java.util.concurrent.TimeUnit;

import static org.learning.parallelprocessor.framework.ThreadPool.async;

public class Launcher {
    public static final ThreadPrintStream THREADPRINT = new ThreadPrintStream(System.out);
    public static void launch(Component... components) throws InterruptedException {
        for (Component c : components) {
            try {
                if (c instanceof ISink) {
                    c.run();
                } else {
                    async(() -> {
                        try {
                            c.run();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                }
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
        //TODO implement stop() method.
    }
}
