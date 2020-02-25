package org.apache.samza.lineage;

public interface LineageReporter<T> {

  void start();

  void register(String source);

  void report(T lineage);

  void stop();
}
