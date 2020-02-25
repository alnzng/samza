package org.apache.samza.lineage;

import java.util.Objects;
import org.apache.samza.system.SystemStream;


public class Dataset implements Urn {

  private final SystemStream systemStream;

  public Dataset(SystemStream systemStream) {
    this.systemStream = systemStream;
  }

  public SystemStream getSystemStream() {
    return systemStream;
  }

  @Override
  public String getUrn() {
    return systemStream.getSystem() + "." + systemStream.getStream();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Dataset dataset = (Dataset) o;
    return Objects.equals(systemStream, dataset.systemStream);
  }

  @Override
  public int hashCode() {
    return Objects.hash(systemStream);
  }
}
