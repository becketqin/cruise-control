/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.model.estimator;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * A class that holds the resultant metrics and their corresponding causal metrics.
 */
public class MetricRelationRegistry {
  // A mapping from the resultant metric to their corresponding causal metrics.
  private final Map<Integer, Map<Integer, Integer>> _registrations;

  public MetricRelationRegistry() {
    _registrations = new HashMap<>();
  }

  /**
   * Register a causal relation for regression.
   *
   * The registration assigns a continuous index to each causal metrics for a given resultant metric so the
   * regression model.
   *
   * For example, if a causal relation is represented as :
   * <pre>
   *   <tt>RM = a * CM1 + b * CM2 + c * CM3</tt>
   * </pre>
   * The registry will assign CM1 as index 0, CM2 as index 1 and CM3 as index 3. Note that this assignment is
   * per causal relation. i.e. If the same causal metric is used in another causal relation, the index of the
   * same metric may be different.
   *
   * For each resultant metrics, only one regression can be registered. And the causal metrics cannot repeat.
   *
   * @param resultantMetric the resultant metric.
   * @param causalMetrics the causal metrics for the given resultant metric.
   */
  public void register(int resultantMetric, Collection<Integer> causalMetrics) {
    if (_registrations.containsKey(resultantMetric)) {
      throw new IllegalStateException(String.format("The resultant metric with id %d has already been registered.",
                                                    resultantMetric));
    }
    Map<Integer, Integer> causalMetricToIndexes = new HashMap<>();
    int index = 0;
    for (int causalMetricId : causalMetrics) {
      if (causalMetricToIndexes.containsKey(causalMetricId)) {
        throw new IllegalArgumentException(String.format("Duplicate metric id %d in the causal metrics %s for metric %d",
                                                         causalMetricId, causalMetrics, resultantMetric));
      }
      causalMetricToIndexes.put(causalMetricId, index);
      index++;
    }
    _registrations.put(resultantMetric, causalMetricToIndexes);
  }

  /**
   * Look up the index of a given causal metric for a given resultant metric.
   *
   * @param resultantMetric the resultant metric the causal metric is used for.
   * @param causalMetric the causal metric to look up.
   * @return the index of the given causal metric for the given resultant metric.
   */
  public int index(int resultantMetric, int causalMetric) {
    Map<Integer, Integer> causalMetricIdsToIndexes = _registrations.get(resultantMetric);
    if (causalMetricIdsToIndexes == null) {
      throw new IllegalArgumentException(String.format("Resultant metric id %d is not a registered resultant "
                                                           + "metric. All the registered resultant metrics are %s",
                                                       resultantMetric, _registrations.keySet()));
    }
    Integer index = causalMetricIdsToIndexes.get(causalMetric);
    if (index == null) {
      throw new IllegalArgumentException(String.format("Causal metric id %d is not registered as a causal metric "
                                                           + "for resultant metric id %d. The registered "
                                                           + "causal metrics are %s",
                                                       causalMetric, resultantMetric, causalMetricIdsToIndexes.keySet()));
    }
    return index;
  }
}
