package org.apache.samza.util;

import java.util.Optional;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobLineageConfig;
import org.apache.samza.lineage.LineageFactory;
import org.apache.samza.lineage.LineageReporter;
import org.apache.samza.lineage.LineageReporterFactory;


/**
 * Helper class to help build and emit job lineage data to configured output stream.
 */
public final class JobLineageEmitter {

  public static void emit(Config config) {
    JobLineageConfig lineageConfig = new JobLineageConfig(config);
    Optional<String> lineageReporterFactoryClassName = lineageConfig.getLineageReporterFactoryClassName();
    Optional<String> lineageFactoryClassName = lineageConfig.getLineageFactoryClassName();

    if (!lineageReporterFactoryClassName.isPresent() || !lineageFactoryClassName.isPresent()) {
      return;
    }

    LineageFactory lineageFactory = ReflectionUtil.getObj(lineageFactoryClassName.get(), LineageFactory.class);

    LineageReporterFactory lineageReporterFactory =
        ReflectionUtil.getObj(lineageReporterFactoryClassName.get(), LineageReporterFactory.class);

    LineageReporter lineageReporter = lineageReporterFactory.getLineageReporter(config);
    lineageReporter.start();
    lineageReporter.report(lineageFactory.getLineage(config));
    lineageReporter.stop();
  }
}
