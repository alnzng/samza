package org.apache.samza.lineage;

import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.application.ApplicationUtil;
import org.apache.samza.application.descriptors.ApplicationDescriptor;
import org.apache.samza.application.descriptors.ApplicationDescriptorImpl;
import org.apache.samza.application.descriptors.ApplicationDescriptorUtil;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.system.SystemStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JobLineageFactory implements LineageFactory<JobLineage> {

  public static final Logger LOGGER = LoggerFactory.getLogger(JobLineageFactory.class);

  @Override
  public JobLineage getLineage(Config config) {
    return new JobLineage.Builder(new ApplicationConfig(config).getAppName())
        .inputs(parseInputs(config))
        .outputs(parseOutputs(config))
        .build();
  }

  private Set<Dataset> parseInputs(Config config) {
    return new TaskConfig(config).getAllInputStreams().stream().map(Dataset::new).collect(Collectors.toSet());
  }

  private Set<Dataset> parseOutputs(Config config) {
    StreamConfig streamConfig = new StreamConfig(config);
    ApplicationDescriptorImpl<? extends ApplicationDescriptor> descriptor =
        ApplicationDescriptorUtil.getAppDescriptor(ApplicationUtil.fromConfig(config), config);
    if (descriptor.getOutputDescriptors().isEmpty()) {
      LOGGER.warn("Not specify output descriptors in '{}', skip to capture outputs for job's lineage analysis '{}'",
          descriptor.getAppClass().getCanonicalName(), new ApplicationConfig(config).getAppName());
    }
    return descriptor
        .getOutputDescriptors().values().stream()
        .map(outputDescriptor -> new Dataset(new SystemStream(outputDescriptor.getSystemName(),
            streamConfig.getPhysicalName(outputDescriptor.getStreamId()))))
        .collect(Collectors.toSet());
  }
}
