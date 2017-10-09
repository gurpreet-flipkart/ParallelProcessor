package org.learning.parallelprocessor.framework;

public interface Engine {
    void pause();

    void resume();

    void cancel();

    void start();
}
