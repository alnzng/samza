package org.apache.samza.lineage;

import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.Serde;


public class JobLineageSerde implements Serde<JobLineage> {

  private final JsonSerdeV2<JobLineage> serde = new JsonSerdeV2();

  @Override
  public JobLineage fromBytes(byte[] bytes) {
    return serde.fromBytes(bytes);
  }

  @Override
  public byte[] toBytes(JobLineage lineage) {
    return serde.toBytes(lineage);
  }
}
