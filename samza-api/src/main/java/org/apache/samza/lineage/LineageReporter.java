package org.apache.samza.lineage;

public interface LineageReporter {

  void start();

  void stop();
}
