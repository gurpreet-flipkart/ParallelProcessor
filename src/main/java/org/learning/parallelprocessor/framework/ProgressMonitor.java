package org.learning.parallelprocessor.framework;

public class ProgressMonitor {
    private int length = 0;

    ThreadLocal<Integer> colorLocal = new ThreadLocal<Integer>() {
        int start = 26;

        @Override
        protected Integer initialValue() {
            return start++;
        }
    };

    public void overwrite(String template, Object... data) {
        for (int i = 1; i <= length; i++) {
            System.out.print("\b");
        }
        int color = colorLocal.get();
        template = template.replaceAll("%s", (char) 27 + "[" + color + "m" + "%s" + (char) 27 + "[0m");

        write(template, data);
    }

    public void write(String msg, Object... data) {
        length = msg.length() + 10;
        System.out.printf(msg, data);
    }

    public void writeln(String msg) {
        length = 0;
        System.out.println(msg);
    }
}
