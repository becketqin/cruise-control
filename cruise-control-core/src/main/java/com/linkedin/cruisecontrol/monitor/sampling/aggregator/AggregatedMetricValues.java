/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.monitor.sampling.aggregator;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;


/**
 * The aggregated metric values.
 */
public class AggregatedMetricValues {
  private final Map<Integer, MetricValues> _metricValues;

  /**
   * Create an empty metric values.
   */
  public AggregatedMetricValues() {
    _metricValues = new HashMap<>();
  }

  /**
   * Create an AggregatedMetricValues with the given values by metric ids.
   * @param valuesByMetricId the values of the metrics. The key is the metric id.
   */
  public AggregatedMetricValues(Map<Integer, MetricValues> valuesByMetricId) {
    if (valuesByMetricId == null) {
      throw new IllegalArgumentException("The metric values cannot be null");
    }
    int length = valuesByMetricId.isEmpty() ? -1 : valuesByMetricId.values().iterator().next().length();
    for (MetricValues values : valuesByMetricId.values()) {
      if (length != values.length()) {
        throw new IllegalArgumentException("The metric values must have the same length for each metric. Saw two "
                                               + "different lengths of " + length + " and " + values.length());
      }
    }
    _metricValues = valuesByMetricId;
  }

  /**
   * Get the {@link MetricValues} for hte given metric id
   *
   * @param metricId the metric id to get metric values.
   * @return the {@link MetricValues} for the given metric id.
   */
  public MetricValues valuesFor(int metricId) {
    return _metricValues.get(metricId);
  }

  /**
   * @return the array length of the metric values.
   */
  public int length() {
    return _metricValues.isEmpty() ? 0 : _metricValues.values().iterator().next().length();
  }

  /**
   * Check if the AggregatedMetricValues contains value for any metrics. Note that this call does not verify
   * if the values array for a particular metric is empty or not.
   *
   * @return true the aggregated metric values is empty, false otherwise.
   */
  public boolean isEmpty() {
    return _metricValues.isEmpty();
  }

  /**
   * @return The ids of all the metrics in this cluster.
   */
  public Set<Integer> metricIds() {
    return Collections.unmodifiableSet(_metricValues.keySet());
  }

  /**
   * Add the metric value to the given metric id. If the metric values for the given metric already exists, it will
   * add the given metric values to it.
   * @param metricId the metric id the values associated with.
   * @param metricValuesToAdd the metric values to add.
   */
  public void add(int metricId, MetricValues metricValuesToAdd) {
    if (metricValuesToAdd == null) {
      throw new IllegalArgumentException("The metric values to be added cannot be null");
    }
    if (!_metricValues.isEmpty() && metricValuesToAdd.length() != length()) {
      throw new IllegalArgumentException("The existing metric length is " + length() + " which is different from the"
                                             + " metric length of " + metricValuesToAdd.length() + " that is being added.");
    }
    MetricValues metricValues = _metricValues.computeIfAbsent(metricId, id -> new MetricValues(metricValuesToAdd.length()));
    metricValues.add(metricValuesToAdd);
  }

  /**
   * Add another AggregatedMetricValues to this one.
   *
   * @param other the other AggregatedMetricValues.
   */
  public void add(AggregatedMetricValues other) {
    for (Map.Entry<Integer, MetricValues> entry : other.metricValues().entrySet()) {
      int metricId = entry.getKey();
      MetricValues otherValuesForMetric = entry.getValue();
      MetricValues valuesForMetric = _metricValues.computeIfAbsent(metricId, id -> new MetricValues(otherValuesForMetric.length()));
      if (valuesForMetric.length() != otherValuesForMetric.length()) {
        throw new IllegalStateException("The two values arrays have different lengths " + valuesForMetric.length()
                                            + " and " + otherValuesForMetric.length());
      }
      valuesForMetric.add(otherValuesForMetric);
    }
  }

  /**
   * Subtract another AggregatedMetricValues from this one.
   *
   * @param other the other AggregatedMetricValues to subtract from this one.
   */
  public void subtract(AggregatedMetricValues other) {
    for (Map.Entry<Integer, MetricValues> entry : other.metricValues().entrySet()) {
      int metricId = entry.getKey();
      MetricValues otherValuesForMetric = entry.getValue();
      MetricValues valuesForMetric = valuesFor(metricId);
      if (valuesForMetric == null) {
        throw new IllegalStateException("Cannot subtract a values from a non-existing MetricValues");
      }
      if (valuesForMetric.length() != otherValuesForMetric.length()) {
        throw new IllegalStateException("The two values arrays have different lengths " + valuesForMetric.length()
                                            + " and " + otherValuesForMetric.length());
      }
      valuesForMetric.subtract(otherValuesForMetric);
    }
  }

  /**
   * Clear all the values in this AggregatedMetricValues.
   */
  public void clear() {
    _metricValues.clear();
  }

  /**
   * Write this AggregatedMetricValues to the output stream to avoid string conversion.
   *
   * @param out the OutputStream.
   * @throws IOException
   */
  public void writeTo(OutputStream out) throws IOException {
    OutputStreamWriter osw = new OutputStreamWriter(out, StandardCharsets.UTF_8);
    osw.write("{%n");
    for (Map.Entry<Integer, MetricValues> entry : _metricValues.entrySet()) {
      osw.write(String.format("metricId:\"%d\", values:\"", entry.getKey()));
      entry.getValue().writeTo(out);
      osw.write("}\"");
    }
  }

  @Override
  public String toString() {
    StringJoiner joiner = new StringJoiner("\n", "{", "}");
    for (Map.Entry<Integer, MetricValues> entry : _metricValues.entrySet()) {
      joiner.add(String.format("metricId:\"%d\", values:\"%s\"", entry.getKey(), entry.getValue()));
    }
    return joiner.toString();
  }

  private Map<Integer, MetricValues> metricValues() {
    return _metricValues;
  }
}
