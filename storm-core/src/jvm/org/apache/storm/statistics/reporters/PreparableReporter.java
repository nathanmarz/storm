package org.apache.storm.statistics.reporters;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;

import java.io.Closeable;
import java.util.Map;


public interface PreparableReporter<T extends Reporter & Closeable> {
  public abstract void prepare(MetricRegistry metricsRegistry, Map stormConf);
  public abstract void start();
  public abstract void stop();

}
