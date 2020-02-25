package org.apache.samza.lineage;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;


public class JobLineage implements Lineage {

  private final String jobName;
  private final Set<Dataset> inputs;
  private final Set<Dataset> outputs;

  public JobLineage(String jobName, Set<Dataset> inputs, Set<Dataset> outputs) {
    this.jobName = jobName;
    this.inputs = inputs;
    this.outputs = outputs;
  }

  @Override
  public String getUrn() {
    return jobName;
  }

  @Override
  public Set<Dataset> getInputs() {
    return inputs;
  }

  @Override
  public Set<Dataset> getOutputs() {
    return outputs;
  }

  public static class Builder {
    private final String jobName;
    private Set<Dataset> inputs;
    private Set<Dataset> outputs;

    public Builder(String jobName) {
      this.jobName = jobName;
      this.inputs = new HashSet<>();
      this.outputs = new HashSet<>();
    }

    public Builder input(Dataset input) {
      if (!Objects.isNull(input)) {
        this.inputs.add(input);
      }
      return this;
    }

    public Builder inputs(Set<Dataset> inputs) {
      if (!Objects.isNull(inputs)) {
        this.inputs.addAll(inputs);
      }
      return this;
    }

    public Builder output(Dataset output) {
      if (!Objects.isNull(output)) {
        this.outputs.add(output);
      }
      return this;
    }

    public Builder outputs(Set<Dataset> outputs) {
      if (!Objects.isNull(outputs)) {
        this.outputs.addAll(outputs);
      }
      return this;
    }

    public JobLineage build() {
      return new JobLineage(jobName, inputs, outputs);
    }
  }
}
