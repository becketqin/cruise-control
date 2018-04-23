/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.cruisecontrol.metricdef;

import java.util.Objects;


/**
 * The metric information including the name, id, the way of interpretation and the metric group name.
 */
public class MetricInfo implements Comparable<MetricInfo> {
  private final String _name;
  private final int _id;
  private final AggregationFunction _aggregationFunction;
  private final String _group;

  public MetricInfo(String name, int id, AggregationFunction aggregationFunction, String group) {
    _name = name;
    _id = id;
    _aggregationFunction = aggregationFunction;
    _group = group;
  }

  /**
   * @return The name of the metric.
   */
  public String name() {
    return _name;
  }

  /**
   * @return the id of the metric.
   */
  public int id() {
    return _id;
  }

  /**
   * @return the {@link AggregationFunction} of the metric.
   */
  public AggregationFunction aggregationFunction() {
    return _aggregationFunction;
  }

  /**
   * @return the metric group of this metric. The metric group is used to aggregate the metrics of the same kind.
   * @see MetricDef
   */
  public String group() {
    return _group;
  }

  @Override
  public String toString() {
    return String.format("(name=%s, id=%d, aggregationFunction=%s)", _name, _id, _aggregationFunction);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_name, _id, _aggregationFunction);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof MetricInfo)) {
      return false;
    }
    MetricInfo o = (MetricInfo) obj;
    return _name.equals(o.name()) && _id == o.id() && _aggregationFunction == o.aggregationFunction();
  }

  @Override
  public int compareTo(MetricInfo o) {
    return Integer.compare(_id, o.id());
  }
}
