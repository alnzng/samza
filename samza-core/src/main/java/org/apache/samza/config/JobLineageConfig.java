package org.apache.samza.config;

import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JobLineageConfig extends MapConfig {

  public static final Logger LOGGER = LoggerFactory.getLogger(JobLineageConfig.class);

  // class name for lineage factory
  public static final String LINEAGE_FACTORY = "job.lineage.factory";
  // class name for lineage reporter factory
  public static final String LINEAGE_REPORTER_FACTORY = "job.lineage.reporter.factory";
  // output stream
  public static final String LINEAGE_REPORTER_STREAM = "job.lineage.reporter.stream";


  public JobLineageConfig(Config config) {
    super(config);
  }

  public Optional<String> getLineageFactoryClassName() {
    return Optional.ofNullable(get(LINEAGE_FACTORY));
  }

  public Optional<String> getLineageReporterFactoryClassName() {
    return Optional.ofNullable(get(LINEAGE_REPORTER_FACTORY));
  }

  public Optional<String> getLineageReporterStreamName() {
    return Optional.ofNullable(get(LINEAGE_REPORTER_STREAM));
  }
}
