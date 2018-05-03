/*
 * Copyright 2018 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals.internals;

public class DoubleWrapper {
  double _value;

  public DoubleWrapper(double value) {
    _value = value;
  }

  @Override
  public String toString() {
    return Double.toString(_value);
  }
}
