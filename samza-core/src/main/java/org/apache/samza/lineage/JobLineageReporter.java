package org.apache.samza.lineage;

import org.apache.samza.serializers.Serde;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;


public class JobLineageReporter implements LineageReporter<JobLineage> {

  private final SystemProducer systemProducer;
  private final SystemStream systemSystem;
  private final Serde<JobLineage> serde;

  private String source;


  public JobLineageReporter(SystemProducer systemProducer, SystemStream systemSystem, Serde<JobLineage> serde) {
    this.systemProducer = systemProducer;
    this.systemSystem = systemSystem;
    this.serde = serde;
  }

  @Override
  public void start() {
    systemProducer.start();
  }

  @Override
  public void register(String source) {
    this.source = source;
    systemProducer.register(source);
  }

  @Override
  public void report(JobLineage lineage) {
    systemProducer.send(source, new OutgoingMessageEnvelope(systemSystem, serde.toBytes(lineage)));
  }

  @Override
  public void stop() {
    systemProducer.stop();
  }
}
