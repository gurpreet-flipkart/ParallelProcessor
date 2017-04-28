package org.learning.parallelprocessor.framework;


import static org.learning.parallelprocessor.framework.ThreadPool.async;

public class Launcher {
    public static final ThreadPrintStream THREADPRINT = new ThreadPrintStream(System.out);
    public static void launch(Component... components) {
        for (Component c : components) {
            try {
                if (c instanceof Sink) {
                    c.start();
                } else {
                    async(() -> {
                        try {
                            c.start();
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
