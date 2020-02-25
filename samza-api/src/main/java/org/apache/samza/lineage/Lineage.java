package org.apache.samza.lineage;

import java.util.Set;


public interface Lineage extends Urn {
  Set<Dataset> getInputs();
  Set<Dataset> getOutputs();
}
