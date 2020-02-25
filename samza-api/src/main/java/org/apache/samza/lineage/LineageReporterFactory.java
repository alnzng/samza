package org.apache.samza.lineage;

import org.apache.samza.config.Config;


public interface LineageReporterFactory<T> {

  LineageReporter<T> getLineageReporter(Config config);
}
