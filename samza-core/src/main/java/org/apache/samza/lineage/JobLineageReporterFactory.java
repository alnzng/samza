package org.apache.samza.lineage;

import org.apache.samza.config.Config;


public class JobLineageReporterFactory implements LineageReporterFactory {
  @Override
  public LineageReporter<JobLineage> getLineageReporter(Config config) {
    return null;
  }
}
