package org.apache.samza.lineage;

import org.apache.samza.config.Config;

public interface LineageFactory<T> {

  T getLineage(Config config);
}
